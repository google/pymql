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

from mw.log import LOG
from mw.api.content import Content
from mw.api.content import TextContent

######################################################################

#
#  html escaping
#
#  originally from mw/client/escaping.py
#  duplicated in mw/mql/grquoting.py
#

from xml.sax import saxutils
import cgi


def quote_attr(data):
    '''
    Prepares data to be used as an attribute value. The return value
    is a quoted version of data. The resulting string can be used
    directly as an attribute value:
    >>> print "<element attr=%s>" % quoteattr("ab ' cd \" ef")
    <element attr="ab ' cd &quot; ef">
    '''
    return (saxutils.quoteattr(data))

def quote_text(data):
    '''
    Convert the characters "&", "<" and ">" in data to HTML-safe
    sequences.
    '''
    return (cgi.escape(data))





######################################################################
#
#   HTML
#

def in_table(p):
    if p is None:         # hit the root of the document
        return False
    elif p.name == u'table':  # hit a table
        return True
    else:
        return in_table(p.parent)
                            
class HTMLContent(TextContent):
    """
    we have special knowledge of html.
    """

    @staticmethod
    def create(html, media_type='text/html', text_encoding='utf-8'):
        if isinstance(html, unicode):
            html = html.encode(text_encoding)
        c = Content(media_type=media_type,
                    text_encoding=text_encoding,
                    length=len(html))
        c.set_body(html)
        return HTMLContent(c)

    @staticmethod
    def match(c):
        """
        true if this ContentWrapper subclass applies to the content argument.

        this function should match if there is *ANY CHANCE* that the browser
        will interpret the content as html.  in some cases this may be
        out of our control - we don't have a description of the content-type
        resolution algorithm in the browser.                            
        """
        if c.media_type not in 'text/html application/xhtml+xml'.split():
            return False
        return True


    @staticmethod
    def remove_element(soup, tag_name):
        elements = soup.findAll(tag_name)
        if elements is not None:
            [element.extract() for element in elements]

    @staticmethod
    def remove_references(soup):
        
        # remove references in soup
        elements = soup.findAll('sup')
        if not elements:
            return

        import BeautifulSoup
        for e in elements:
            if isinstance(e, BeautifulSoup.Tag) and (u'class', u'reference') in e.attrs:
                e.extract()

    def make_soup(self):
        """
        parse the html using BeautifulSoup
        """

        kws = {}
        if self.content.text_encoding is not None:
            kws['fromEncoding'] = self.content.text_encoding
        import BeautifulSoup        
        soup = BeautifulSoup.BeautifulSoup(self.content.body, **kws)

        assert soup is not None

        return soup

    def guess(self, soup):
        """
        if we ran beautifulsoup, it may have figured out more
        about the document.  incorporate if so...
        """
        # beautifulsoup may have figured out the text encoding
        if self.content.text_encoding is None:
            self.content.text_encoding = soup.originalEncoding
        else:
            # for now, go with the explict header given by the
            # server, assuming that some transcoding tool somewhere
            # along the line probably didn't have deep enough knowledge
            # of sgml to change the explicit encoding in the file.
            if self.content.text_encoding != soup.originalEncoding:
                LOG.warn('format.html.guess', 'html encoding mismatch for content %s: inferred %s'
                    % (self.content, soup.originalEncoding))

        # get suggested concept name from title?
        # title = soup.head.title.string


    def plain_content(self):
        """
        strip out all all tags and return just the plain text.
        """
        soup = self.make_soup()
        assert soup is not None
        import BeautifulSoup

        stripped = ''.join([e for e in soup.recursiveChildGenerator()
                            if isinstance(e,unicode) and not isinstance(e, BeautifulSoup.Comment)])
        # sanitize.
        from mw.util.html_utils import sanitize_html
        shtml = sanitize_html(stripped, encoding='utf-8')

        c = Content(media_type='text/plain',
                    text_encoding=self.text_encoding,
                    length=len(shtml))
        c.set_body(shtml)
        return c


    # used to scan for word boundaries when making blurbs.
    # blurbs should move to PlainTextContent or something?
    word_re = re.compile('\W')
    dspace_re = re.compile(u' +', re.IGNORECASE)
    dpara_re = re.compile(u'\xb6+', re.IGNORECASE)
    imgmap_re = re.compile(u'&lt;imagemap&gt;', re.IGNORECASE)
    image_re = re.compile(u'image:[^.]+\.[^.][^.][^.]')
    pilcrow_re = re.compile(u'^\xb6+')

    # TODO: move to html5lib
    def to_blurb(self, maxlen=500):
        """
        A simple algorithm to just get a simple blurb
        - find the first paragraph not in a table
        - strip out all enclosing tables, links, etc
        """
        import BeautifulSoup
        soup = self.make_soup()
        assert soup is not None

        # strip out some elements first
        # (find the first paragraph not in a table)
        self.remove_element(soup, 'table')
        self.remove_element(soup, 'script')
        self.remove_references(soup)

        plist = soup.findAll('p')
        if plist is None or len(plist) == 0:
            srclist = [soup]
            has_para = False
        else:
            srclist = plist
            has_para = True

        l = []
        total_len = 0
        for src in srclist:
            for e in src.recursiveChildGenerator():
                if total_len >= maxlen:
                    break

                # keep anchors but replace them with our span
                if 0 and isinstance(e, BeautifulSoup.Tag) and (e.name == 'a'):
                    # XXX this disables our anchor replacement code. the
                    # spans are showing up on the front end and we don't
                    # seem to use them for anything so far. when we do
                    # we can reenable it.
                    if e.string is None:
                        continue
                    s = u'<span class="BlurbLink">%s</span> ' % e['href']
                    total_len += len(e['href'])
                    if total_len <= maxlen:
                        l.append(s)
                    continue

                # append text, and keep an eye on the maxlen limit
                if isinstance(e,unicode) and not isinstance(e, BeautifulSoup.Comment):
                    s = e.string
                    slower = s.lower().strip()
                    if (not slower or
                        slower.startswith(u'image:link') or
                        slower.startswith(u'this is a featured article. ') or
                        slower.startswith(u'desc none') or
                        slower.startswith(u'&lt;imagemap&gt;') or
                        slower.endswith(u'&lt;/imagemap&gt;') or 
                        self.image_re.match(slower)):
                        continue
                    total_len += len(s)
                    if total_len <= maxlen:
                        # workaround for bz 4966
                        if (len(l) > 0 and l[-1][-1].isalnum() and
                            s[-1].isalnum()):
                            l.append(u' ')
                            total_len += 1
                        l.append(s)
                        #print 'added "%s"' % s
                    else:
                        # add up to the last word before the total length
                        # exceeds maxlen
                        piece = s[0:(maxlen-total_len)]
                        i = 0
                        plen = len(piece)
                        while i<plen:
                            i+= 1
                            # chop off everything after the last
                            # non-alpha-numberic
                            if not piece[-i].isalnum():
                                l.append(piece[:-i])
                                #print 'added "%s"' % piece[:-i]
                                break

            if total_len >= maxlen:
                l.append(u'...')
                break
            if has_para:
                l.append(u'\xb6') # use pilcrow sign instead of para tag
            elif l and not l[-1].endswith(u' '):
                l.append(u' ')

        blurb = u''.join(l)

        # workarounds for some known problems
        if blurb.find(u'  ') > -1:
            blurb = self.dspace_re.sub(u' ', blurb)
        if blurb.find(u'\xb6\xb6') > -1:
            blurb = self.dpara_re.sub(u'\xb6', blurb)
        if blurb.lower().find(u'&lt;imagemap&gt;') > -1:
            blurb = self.imgmap_re.sub(u'', blurb)
        if blurb.startswith(u'\xb6'):
            blurb = self.pilcrow_re.sub(u'', blurb)
        return blurb


def depilcrow(blurb_body, break_paragraphs, content_length):
    # remove or replace pilcrow signs with <p>
    # ME-1126 replace the args to replace with non-unicode characters because
    # python2.4 just cannot resist from misbehaving badly with UTF-8 input.
    # grrr.   
    # XXX: restore u'...' when we move to chronos
    if blurb_body and isinstance(blurb_body, str):
        blurb_body = blurb_body.decode('utf-8')

    if break_paragraphs:
        blurb_body = blurb_body.replace(u'\xb6', u'<p>')
    else:
        blurb_body = blurb_body.replace(u'\xb6', u' ')
    return blurb_body
