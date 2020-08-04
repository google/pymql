#! /usr/bin/env python
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import re
import sys
from lxml import etree
from lxml.html import fromstring
from lxml.html.clean import Cleaner

import html5lib
from html5lib import treebuilders, treewalkers, serializer
from html5lib import sanitizer
from mw.log import LOG
from mw.error import SanitizationError
        
__RE_XML_ENCODING = re.compile(
    ur'^(\s*<\?\s*xml[^>]+)\s+encoding\s*=\s*"([^"]*)"\s*', re.U)

def encoding_declaration(body):
    m = __RE_XML_ENCODING.match(body)
    if m:
        return m.group(2)

def fix_xml_encoding(text_body):
    """
    If the given xml document has an encoding declaration, then
    reencode that document with that declaration. This sucks, but it's
    the only sane way to deal with lxml.html
    """
    encoding = encoding_declaration(text_body)
    if encoding:
        # reencode so lxml can be happy - this totally
        # sucks because we just spent all this time
        # encoding it.
        LOG.notice("content.reencode", "Dumb reencoding of blob body as %s" % encoding)
        text_body = text_body.encode(encoding)
        
    return text_body
    
TAGS_allowlist = ('p', 'span', 'body', 'pre', 'code', 'a', 'b', 'i',
                  'br', 'hr',
                  'u', 'div', 'body', 'dl', 'dd')

def sanitizer_factory(*args, **kwargs):
    san = sanitizer.HTMLSanitizer(*args, **kwargs)
    san.strip_tokens = True
    san.lowercaseElementName = True
    san.lowercaseAttrName = True
    return san

def sanitize_html(data, encoding=None):
    parser = html5lib.HTMLParser(tree=treebuilders.getTreeBuilder("dom"),
        tokenizer=sanitizer_factory)
    walker = treewalkers.getTreeWalker("dom")
    stream = walker(parser.parseFragment(data, encoding=encoding))
    slzr = serializer.htmlserializer.HTMLSerializer(omit_optional_tags=False,
        quote_attr_values=True, use_trailing_solidus=True)
    html = slzr.render(stream, encoding)
    return html

def sanitize_content(content, encoding='utf-8'):
    """
    sanitize any content that could cause a browser to run javascript.
    
    understands html for now.
    XXX what other media types need to be sanitized?
        multipart/* containing html
        rss or atom that contains html
        any other content-types that might be interpreted by the browser?
    """
    data = content.body
    mt = content.media_type

    if mt == 'text/x-cross-domain-policy':
        raise SanitizationError("Cannot retrieve text/x-cross-domain-policy files for security reasons",
                                app_code="/sanitizer/media_type/restricted",
                                media_type=mt)
    # XXX better regex possible, but this should catch known valid ones
    mt_re = re.compile('^[-_.a-z0-9]+/[-_.a-z0-9]+$')
    if mt_re.match(mt) is None:
        raise SanitizationError("Unhandled media type",
            app_code='/sanitizer/media_type/unhandled', media_type=mt)
  
    if mt in ['text/html', 'application/xhtml+xml']:
        data = sanitize_html(data, encoding) 
    else:
        unsafe_media_types = ['text/html', 'application/xhtml+xml',
                            'application/javascript', 'application/data-javascript',
                            'application/ecmascript', 'text/javascript', 'text/ecmascript',
                            'text/css', 'application/atom+xml', 'application/rss+xml']

        if mt.startswith('multipart/') or mt in unsafe_media_types:
            # log warning and move on
            LOG.warn("html.sanitize", "Potentially unsafe content data of type: %s" % mt)

    return data

def new_sanitize_html(html, server_root=None, **kwargs):
    '''
    Cleans a piece of html text and returns an HTMLElement object. 
    kwargs are keyword arguments that lxml.html.clean.Cleaner accepts.
    http://codespeak.net/lxml/api/lxml.html.clean.Cleaner-class.html

    The reason we do not use lxml.html.clean.clean_html which accepts a string
    and returns a string is because clean_html internally converts the string to
    an etree and calls Cleaner.clean_html and there are legitimate use cases
    where we want a etree instead of the text which we would have to convert
    back to an etree again. To get the string back call lxml.html.tostring(doc)
    '''

    # some docs are blank!
    if not html:
        # an empty document
        return fromstring(u"<html></html>")
    
    html = fix_xml_encoding(html)
    
    try:
        doc = fromstring(html)
    except ValueError as e:
        raise SanitizationError("Cannot parse html/xml document: %s" % str(e),
                                app_code='/sanitizer/parse_error')

    # note that if a ParserError is thrown, this document is SO
    # malformed, that not even libxml2 can parse it.
    except (etree.XMLSyntaxError, etree.ParserError) as e:
        # xml syntax errors don't always seem to have data in them
        raise SanitizationError("Cannot parse html/xml document: Syntax error in the document",
            app_code='/sanitizer/parse_error')
    
    for tag in ('title', 'style'):
        [element.drop_tree() for element in doc.iterdescendants(tag)]

    # make absolute urls first, because the cleaner strips JS urls,
    # and those turn into absolute urls
    cleaner = Cleaner(**kwargs)
    cleaner(doc)
    if server_root:
        for element, attr, link, pos in doc.iterlinks():
            if link and not link.startswith("#"):
                element.make_links_absolute(server_root)
    return doc

def consume_subtree(context, depth, tag):
    '''
    Given an element iterator generator and current depth, consumes the tree
    until the initial depth and returns the resulting depth. We don't use
    HTMLElement.drop_tree because it works a bit differently and will not 
    always take out what we want
    '''
    outlevel = depth
    for subaction, subelem in context:
        if subaction == 'start':
            depth += 1
        if subaction == 'end':
            depth -=1
            if subelem.tag == tag and depth + 1 == outlevel:
                break
    return depth

def truncate_etree(node, limit, text_only=True, html_only=True):
    '''
    Truncates an HTML Element Tree upto a maximum of limit characters. This
    returns an HTML string or a text only string NOT an HTMLElement. Callers 
    are responsible to convert them back to an HTMLElement if they so desire.

    Algorithm:
    1 init: depth = -1, threshold = Inf
    2 while walking the tree for each start and end tag:
        2.1 if the element is not allowlisted strip the entire subtree
        2.2 if the element is in the allowlist and depth is less than the
            threshold:
            2.2.1 if end_flag set clear node and move on
            2.2.2 if adding the text (all text upto the first sub-element) will
                  cause the result to go over the limit:
                  2.2.2.1 chop off the text so that it fits the limit
                  2.2.2.2 reset threshold to current depth
                  2.2.2.3 set the end_flag
            2.2.3 add the start/end tag, attributes and text to result
    '''

    depth = -1              # depth = -1 so as to make it 0 on first run
    threshold = sys.maxint  # initial threshold is Inf
    char_count = 0
    res = u''
    end_flag = False

    # get the tree walker
    context = etree.iterwalk(node, events=('start', 'end'))
    for action, elem in context:
        if action == 'start':
            # increment depth
            depth += 1
            if html_only and elem.tag not in TAGS_allowlist:
                depth = consume_subtree(context, depth, elem.tag)
                continue
            if depth < threshold:
                # if adding the text will make the result longer than the limit
                # we replace the element's text with text upto the limit and set
                # the threshold to the current depth. As a result any future
                # allowlisted elements will not be able to contribute to the
                # final output, yet the end tags required for all start tags
                # upto now will still come in (including tail text)
                if end_flag:
                    elem.clear()

                element_text = elem.text or ''
                new_length = char_count + len(element_text)
                ellipses = False
                if new_length >= limit:
                    if new_length > limit:
                        ellipses = True
                        element_text = element_text[:limit - char_count - 3]
                    else:
                        element_text = element_text[:limit - char_count]
                    threshold = depth
                    # find the last space in the text, if there is clear it
                    last_space_idx = element_text.rfind(' ')
                    if last_space_idx < 0:
                        element_text = ''
                        continue
                    end_flag = True
                    element_text = element_text[:last_space_idx]
                char_count += len(element_text)
                # generate the HTML/text
                if not text_only:
                    attrs = ' '.join(['='.join(kv) for kv in \
                        [(k, '"%s"' % v) for k, v in elem.items()]])
                    res += '<%s %s>' % (elem.tag, (attrs or ''))
                res += element_text
                if ellipses:
                    res += "..."
                    char_count += 3
                    
        elif action == 'end':
            if depth < threshold and html_only and elem.tag in TAGS_allowlist:
                if end_flag:
                    elem.clear()
                # also make sure that we only include tail text upto the limit
                element_tail = elem.tail or ''
                ellipses = False
                new_length = char_count + len(element_tail)
                if new_length >= limit:
                    if new_length > limit:
                        ellipses = True
                        element_tail = (element_tail)[:limit - char_count - 3]
                    else:
                        element_tail = (element_tail)[:limit - char_count]
                    threshold = depth
                    # find the last space in the text
                    last_space_idx = element_tail.rfind(' ')
                    # we dont need to clear it in this case simply dropping all
                    # text is good enough
                    if last_space_idx < 0:
                        last_space_idx = 0
                    end_flag = True
                    element_tail = element_tail[:last_space_idx]
                char_count += len(element_tail)
                # generate HTML
                if not text_only:
                    res += '</%s>' % elem.tag
                res += element_tail
                if ellipses:
                    res += "..."
                    char_count += 3
                    
            # decrement depth
            depth -= 1
    return res

