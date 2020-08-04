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

"""

general purpose Content class.

an instance of Content is a wrapper for a chunk of binary
data (the body).  the body way not actually be loaded as
as string - it could be an url to fetch, a file descriptor,
or a reference to a blob in the blob database.

the Content object provides enough metadata to make a useful
http header for the body:
   media_type
   text_encoding (if applicable)
   language code(s)

it also contains information about where the content is stored:
   remote url
   /type/content id for the graph/.clob
   in memory
   through a file descriptor

once you have an instance of Content, you can:
- create a content-type-specific wrapper for type-specific transformations
  like html sanititizing, image resizing, wikipedia markup conversion, etc.
- upload it to the graph/blob, getting a /type/content id

an example of creating a Content object from an HTTP form post
can be found in the /api/service/upload code.

parts of this class (with different implementations) could become
part of an external python metaweb api.  there is enough metadata
involved that it is definitely worth bundling it into an class.

"""

import socket, cStringIO, urllib2, magic, feedparser
import mw.siteconfig
from datetime import datetime
from mw.log import LOG
from mw.error import NetworkAddressError, ContentLoadError
from mw.blob.blob import Blob
from mw.formats.contenttype import MediaType, TextEncoding, ContentType, LanguageCode
from mw.util.attrib import set_oauth_attribution_if_needed

# this is a dummy document to signify that a new document should be created
NEW_DOCUMENT = object()

# Only those mime-types specified here will be checked for validity,
# everything else goes through.
VALIDATED_MIME_TYPES =  {
    "image": ["gif","png","jpeg", "x-icon"],
}

class Content(object):
    """
    python object representing a chunk of data with metadata attached.

    may be initialized from several sources:
      - external url              (external http fetch)
      - /type/content instance    (graph/mql and blob/http fetch)
      - POST                      (from post data)
      - file upload?

    currently this has to keep the whole body in memory.  use it wisely.

    probably want method(s) for hooking up to a form upload too.
    """

    def __init__(self, blob_id=None, language=None, length=None, 
                 media_type=None, text_encoding=None, content_type=None,
                 document_id=None, transaction_id=None):
        
        # mql id of the /type/content object if present/known
        self.content_id = None

        # mql id of a /common/document object if present/known
        # this is not fetched by default, but can be set before writing.
        self.document_id = document_id

        # status if fetch attempted
        # TODO: this needs to be handled via runtime errors that are
        #       handled by the calling code - this is too hacky and brittle
        self.status = None

        # download url if fetched
        self._source_url = None

        # may be needed to load older wikipedia blurbs
        self._document_guid = None
        
        # go through the proxy?
        self._use_proxy = True

        # headers if fetched (note that order sent by server isn't preserved)
        self._fetch_header_text = None

        # filename if uploaded
        self._fetch_filename = None

        # timestamp when fetched
        self._fetch_timestamp = None

        # is the content editable? (the content is not editable if it's
        # a temporary local copy of some external content.)
        self._editable = True

        # binary string if available
        self.body = None

        # readonly file-like object if available
        self.file = None

        # name of uploaded file
        self.filename = None

        #
        # these have the same names as in /type/content
        #
        self.blob_id = blob_id

        # this is the length in bytes (not chars) - integer if available
        self.length = length

        # this is the language - could be multiples
        if language is None:
            self.language = []
        elif isinstance(language, list):
            self.language = language
        elif isinstance(language, basestring):
            self.language = [language]
        else:
            assert False, 'bad language= argument to Content()'
            
        self.media_type = media_type
        self.text_encoding = text_encoding

        if content_type is not None:
            ct = ContentType(content_type)
            assert media_type is None
            self.media_type = ct[0]
            if ct[1] is not None:
                assert text_encoding is None
                self.text_encoding = ct[1]

        self.rights_holder = None

        self.license = None
        
        self.transaction_id = transaction_id


    def __getitem__(self, key):
        # XXX impersonate a dict.  should do this better
        if key == 'id':
            return self.content_id
        raise KeyError

    def get_proxy_opener(self, mss):
        """
        Lazily retrieve proxy info
        """
        if not hasattr(self, 'proxy_opener'):
            proxy_addr = mss.config.get('me.external_proxy')
            if proxy_addr is None or proxy_addr == '':
                return None
            proxy_handler = urllib2.ProxyHandler({'http': proxy_addr})
            self.proxy_opener = urllib2.build_opener(proxy_handler).open

        return self.proxy_opener

    def unicode_body(self, mss=None):
        # note you'd better have a valid self.text_encoding!
        if self.body is None:
            self.fetch_body(mss=mss)
        return unicode(self.body, self.text_encoding)

    def set_body(self, body):
        assert isinstance(body, basestring)

        if isinstance(body, str):
            self.body = body
        elif isinstance(body, unicode):
            # default to utf8
            if self.text_encoding is None:
                self.text_encoding = TextEncoding('utf-8')
            self.body = body.encode(self.text_encoding.codec)
        self.length = len(self.body)

    def set_file(self, file, filename=None):
        assert self.file is None
        assert self.body is None
        self.file = file
        self.filename = filename

    def content_type_header(self):
        ct = self.media_type
        enc = self.text_encoding
        if ct is None:
            return ''

        if enc is not None:
            return '%s; charset=%s' % (ct, enc)

        # wsgi wants a real string
        return str(ct)
        
    def __str__(self):
        return '<Content length=%s media_type=%s charset=%s>' % (self.length, self.media_type, self.text_encoding)


    def parse_http_header(self, k, v):
        """
        parse an http header pair
        """
        k = k.lower()
        if v is None: return
        
        if k == 'content-type':
            mt, te = ContentType(v)
            if mt is not None:
                self.media_type = mt
            if te is not None:
                self.text_encoding = te

        elif k == 'content-length':
            self.length = int(v)

        elif k == 'content-language':
            self.language += [LanguageCode(lang.strip())
                              for lang in v.split(',')]


    # convert an ip from a string to an int
    #
    @staticmethod
    def ipstr_to_int(ip):
        iplist = ip.split('.')
        if len(iplist) != 4:
            raise NetworkAddressError('invalid ip: %s' % ip, 
                app_code='/content/invalid/net_add_format', ip=ip)
        net = 0
        shift_factor = 3
        for ip in iplist:
            net += (int(ip) << (shift_factor * 8))
            shift_factor -= 1

        return net

    # only accepts network addr as ip for now. convert a network spec.
    # accept an optional mask after the network ip. e.g. 192.168.0.0/16
    #
    @staticmethod
    def parse_network(net_mask):
        netmask_list = net_mask.split('/')
        if len(netmask_list) > 2:
            raise NetworkAddressError('invalid network/mask spec: %s' % net_mask, 
                app_code='/content/invalid/net_mask_format', net_mask=net_mask)

        if len(netmask_list) == 1:
            netstr = net_mask
            mask = 0xffffffff
        else:
            netstr = netmask_list[0]
            masklen = int(netmask_list[1])
            if (masklen < 0) or (masklen > 32):
                raise NetworkAddressError('invalid net mask: %d' % masklen,
                    app_code='/content/invalid/net_mask_format', masklen=masklen)
            shift_size = 31
            mask = 0
            for i in range(masklen):
                mask += (1 << shift_size)
                shift_size -= 1

        net = Content.ipstr_to_int(netstr)
        return net, mask

    @staticmethod
    def is_internal_host(hostname):
        try:
            ipstr = socket.gethostbyname(hostname)
        except socket.gaierror:
            # no such host, let the internal query fail
            return False
        # check against classes A, single B, and C
        if (ipstr == '127.0.0.1' or ipstr.startswith('192.168.') or
            ipstr.startswith('169.254.') or ipstr.startswith('10.')):
            return True

        # contiguous class B
        ips = [int(n) for n in ipstr.split('.')]
        if (ips[0] == 172) and (ips[1] >= 16) and (ips[1] <= 31):
            return True

        # check against 208.68.108.0/22 (see bug #2040)
        mwnet = (208 << 24) + (68 << 16) + (108 << 8)
        mask = (0xff << 24) + (0xff << 16) + (0xfc << 8)

        mwnet, mask = Content.parse_network('208.68.108.0/22')
        net = Content.ipstr_to_int(ipstr)
        return ((net & mask) == mwnet)


    def _fetch(self, mss=None, parse_headers=True, fetch_body=True):
        """
        Actually do the fetch - go out to the network, but making only
        local connections. One of:
        
        1) via a local proxy which goes out onto the internet
        2) to an internal CDB server
        3) To an internal content server like wikipedia
        """

        url = self._source_url
        opener = None
        # XXX must set User-Agent or wikipedia gives us 403 Forbidden!
        headers = {'user-agent':'Mozilla 5'}
        if self._use_proxy and mss:
            LOG.notice("content", "Retrieving %s via proxy" % url)
            opener = self.get_proxy_opener(mss)
        
        if opener is None: # no proxy ok we get a direct connection
            LOG.info("content", "Retrieving %s without proxy" % url)
            if self.transaction_id is not None:
                headers['X-Metaweb-TID'] = self.transaction_id
            opener = urllib2.urlopen
            
        # unfortunately this is the only way to set a timeout (globally)
        DEF_BLOB_HTTP_TIMEOUT = 3.0 # in seconds
        if opener is not None: # we are probably going outside our garden
            DEF_BLOB_HTTP_TIMEOUT = 15.0
        dto = socket.getdefaulttimeout()
        try:
            try:
                socket.setdefaulttimeout(DEF_BLOB_HTTP_TIMEOUT)
                req = urllib2.Request(url, headers=headers)
                response = opener(req)
            except urllib2.HTTPError, e:
                # severity here really varies.  for external content this
                # is expected, but for internal clobs it's bad news.
                LOG.warning("content", 'error fetching url %s via %s' % (self._source_url, url), error=str(e))
                self.status = "%s %s" % (e.code, e.msg)
                # we've already logged the internal error, so
                # propagate up the fake url
                e.source_url = self._source_url
                e.close()
                result_code = e.code

                # force error to 400, because this data is outside our system
                raise ContentLoadError('Failed to fetch URL', http_code=400,
                    app_code='/url_submit/external_uri/http_failure', inner_exc=e,
                    uri=self._source_url, http_res_code=e.code, http_res_msg=e.msg)
            except urllib2.URLError, e:
                self.status = "502 %s" % 'Bad Gateway. ' + str(e)
                raise ContentLoadError('Failed to fetch URL', http_code=502,
                    app_code='/url_submit/external_uri/fetch_failure', 
                    uri=self._source_url, inner_exc=e, 
                    # this is fugly - URLError needs to get its act together
                    error=str(e))
        finally:
            socket.setdefaulttimeout(dto)

        self.status = "%s %s" % (response.code, response.msg)

        self._fetch_header_text = \
                    ''.join(['%s: %s\n' % pair
                             for pair in sorted(response.info().items())])

        if parse_headers:
            for k,v in response.info().items():
                self.parse_http_header(k, v)

        self.body = None

        if fetch_body and self.status.startswith('200'):
            self.set_body(response.read())

        # urllib2 not closing sockets correctly:
        # http://mail.python.org/pipermail/python-bugs-list/2007-January/036554.html
        response.fp._sock.close()
        response.close()


    def _body_append(self, buf):
        # XXX good place to compute blob_id if we don't already have it
        self.body += buf

    def fetch(self, url, mss=None):
        self._source_url = url
        self._fetch(mss=mss, parse_headers=True, fetch_body=True)

    def fetch_body(self, mss=None):
        """
        fetch the body if we already have the metadata
        """
        if self.body is not None:
            return

        if self.status is not None and not self.status.startswith('200'):
            return

        if self.file is not None:
            if self.length is not None:
                # XXX should stream this in chunks?
                self.body = self.file.read(self.length)
            else:
                self.body = self.file.read()
                self.length = len(self.body)

            return

        # check if this is a "virtual" wikipedia article blob
        if not self.blob_id and self._source_url:
            self.blob_id = Content.doc_guid_to_blob_id(self)
            if self.blob_id:
                # XXX we can now only assume that this is an html page.
                # this may change later but we do not have any other info stored
                if not self.media_type:
                    self.media_type = 'text/html'
            LOG.notice('content.fetch.body',
                       'using derived static blob_id %s' % self.blob_id)

        if self.blob_id is not None:
            if mss:
                # uses BlobClient now so that we get the benefit
                # of random reconnects
                self.body = mss.ctx.blobd.get_blob(self.blob_id, tid=mss.transaction_id)
                self.length = len(self.body)
                self.status = '200 OK'
                return

            config = mw.siteconfig.get_config2()
            hostlist = mw.siteconfig.get_addr_list2(config, 'clobd.address')
            # XXX is this ever called? i think clobd.address is now a list
            host, port = hostlist[0]
            self._source_url = ("http://%s:%d/mw/clob/getclob?blobkey=%s"
                                % (host, port, self.blob_id))

            # don't go through the proxy for clob requests
            self._use_proxy = False

            # fall through to self._source_url handling

        if self._source_url is not None:
            # should we re-parse the headers?
            self._fetch(mss=mss, parse_headers=False, fetch_body=True)
            return

        # we really need a way to detect an error so we can say:
        # raise ContentLoadError, 'no source for content body'

        # but this particular state can happen if we hit a document
        # with no attached content node. For that we'll return a
        # zero-length content
        self.body = ""
        self.length = 0
        self.status = "200 No body"
        self.media_type = MediaType("text/plain")
    

    @staticmethod
    def mqlquery(**kws):
        d = dict(id=None,
                 name=None)
        fq_props = {
            'limit': 1,
            '/type/content/blob_id': None,
            '/type/content/media_type': None,
            '/type/content/text_encoding': None,
            '/type/content/length': None,
            '/type/content/language': None
        }
        d.update(fq_props)
        d.update(kws)
        return d

    @staticmethod
    def mqlquery_import(**kws):
        d = dict(type='/type/content_import',
                 id=None,
                 import_time=None,
                 uri=None,
                 filename=None,
                 header_blob_id=None,
                 content=None)
        d.update(kws)
        return d

    # generate a static blob id based on a document guid
    @staticmethod
    def doc_guid_to_blob_id(cinfo, is_blurb=False):

        doc_guid = cinfo._document_guid
        # strip out id or guid prefix
        for prefix in ('#', '/guid/'):
            if doc_guid.startswith(prefix):
                guid = doc_guid[len(prefix):]
                break
        else:
            guid = doc_guid
            LOG.warn("content.doc_guid_to_blob_id",
                     "Unknown guid format for doc_guid, using it raw",
                     doc_guid=doc_guid)

        if cinfo._source_url is None:
            raise ContentLoadError(
                'Request for static content without corresponding external id.', 
                app_code='/trans/invalid_content', guid=guid)
        wpid = cinfo._source_url 
        wpid = wpid.replace('http://wp/en/', '')
        if is_blurb:
            blob_id = 'static:mw-render/en/blurbs/%s' % wpid
        else:
            blob_id = 'static:mw-render/en/%s' % wpid

        return blob_id


    def load_info(self, mss, id):
        """
        load content info from the graph

        you can call fetch_body() after this to get the body
        from the blob if needed.
        """
        # mss is for initialization only -
        # do not stash a copy of ctx here!
        # for now let's keep this class ctx-independent
        #  and see if that can be made to work

        self.transaction_id = mss.transaction_id
        result = mss.mqlread({'id': id, 'type': [],
                              '/type/content/blob_id': None})

        if result is None:
            self.status = '404 Not Found'
            return

        types = result['type']
        blob_id = result.get('/type/content/blob_id')

        LOG.info("content", 'content.py types: %s %s' % (id, types))

        # change this look for blob_id
        if '/type/content' in types or blob_id is not None:
            mss.add_hint('content')
            self.content_id = id
            result = mss.mqlread(Content.mqlquery(id=id,
                                                  name= { "optional": True,
                                                          "value":None}))
            self.load_mql(mss, result)
        elif '/type/content_import' in types:
            # not sure what kind of hint to add here
            self.content_id = id
            result = mss.mqlread(Content.mqlquery_import(id=id))
            self.load_mql_import(mss, result)
        elif '/common/document' in types:
            mss.add_hint('document')
            self.document_id = id
            q = {"id": id,
                 "guid": None,
                 "type": "/common/document",
                 "content": Content.mqlquery(optional=True),
                 "source_uri": {
                    "value": None, "optional": True
                    }
                 }
            result = mss.mqlread(q)
            LOG.notice("content", 'content.py document', q=q, result=result)
            self.load_document(mss, result)

        else:
            raise ContentLoadError("%s is not a valid document or content object" % id, 
                app_code='/content/id_resolution/invalid_id', id=id)


    @classmethod
    def from_metaweb_id(cls, mss, id):
        c = cls()
        c.load_info(mss, id)
        return c

    @classmethod
    def from_mql(cls, mss, result):
        c = cls()
        c.load_mql(mss, result)
        return c

    
    def load_mql(self, mss, result):
        self.length = int(result['/type/content/length'])

        self.media_type = MediaType.from_id(result['/type/content/media_type'])
        # certain mime classes get cached in different ways
        if self.media_type:
            mss.add_hint(str(self.media_type.split('/')[0]))
        self.text_encoding = TextEncoding.from_id(result['/type/content/text_encoding'])

        # should be a list
        self.language = result['/type/content/language']

        self.blob_id = result['/type/content/blob_id']

        # self.body is still None, but HEAD is OK.
        #  using http status codes for self.status was far too simple-minded.
        self.status = '200 OK'

    @classmethod
    def from_document(cls, mss, result):
        c = cls()
        c.load_document(mss, result)
        return c

    def load_document(self, mss, result):
        # source_uri trumps content
        # normalize content and source_uri key 
        if result.has_key('/common/document/content'):
            result['content'] = result['/common/document/content']
        if result.has_key('/common/document/source_uri'):
            result['source_uri'] = result['/common/document/source_uri']

        self._document_guid = result.get('guid') or result.get('id')
        if result.get('source_uri'):
            self._editable = False
            source_url = result['source_uri']
            if isinstance(source_url, dict):
                source_url = source_url['value']
            self._source_url = source_url
            mss.add_hint('external')
        elif result.get('content') is not None:
            if 'guid' in result['content']:
                self.content_id = '/guid/%s' % result['content']['guid'][1:]
            else:
                self.content_id = result['content']['id']
            self.load_mql(mss, result['content'])
        

    # XXX this is hacky. we may need a ContentImport class to deal with
    # this eventually.
    def load_mql_import(self, mss, result):
        LOG.info("content", 'mqlquery_import result: %s' % result)
        self.blob_id = result['header_blob_id']
        self.media_type = MediaType.from_id('/media_type/text/plain')

        # add the hint for the type of object loaded
        mss.add_hint(self.media_type.split('/')[0])
        self.text_encoding = 'ascii'
        self.status = '200 OK'


    def upload_fetch_record(self, mss):
        """
        create a content import concept to track a BLOB fetch or upload.

        this does nothing unless self.content_import was set by a previous
        url fetch.

        return the id of the content import concept created, or None
        """
        if self._fetch_header_text is None:
            return None
        
        # store the header block
        # this isn't done through an instance of Content because
        #  we don't create full /type/content objects for header blocks...

        d = dict(id=None,
                 type='/type/content_import',
                 content=dict(id=self.content_id),
                 create='unconditional')

        if self._fetch_timestamp is not None:
            d['import_time'] = str(self._fetch_timestamp)
        if self._source_url is not None:
            assert isinstance(self._source_url, basestring)
            d['uri'] = dict(value=self._source_url)
        if self._fetch_filename is not None:
            d['filename'] = self._fetch_filename

        result = mss.mqlwrite(d)
        return result['id']


    def validate_file_type(self, f):
        """
        verify that the passed in buffer is not part of a restricted set
        that needs to be validated based on first 4k bytes in the file. 
        only validate against VALIDATED_MIME_TYPES list. If the provided
        media type is not the same as sniffed, raise exception
        """
        
        cur_pos = f.tell()
        buf = f.read(1024)
        f.seek(cur_pos)
        found_type = magic.whatis(buf).lower()
        type_parts = found_type.split("/")
        if len(type_parts) < 2:
            raise ContentLoadError('Invalid file format', 
                                   app_code='/content/file_format/invalid_mime_type', 
                                   mime_type=found_type)

        # not allowed to specify an encoding for images
        if self.text_encoding:
            LOG.warn("upload.image.encoding", 
                     "Image was uploaded with a character encoding", 
                     encoding=self.text_encoding)
            self.text_encoding = None
                
        type_class = type_parts[0].strip()
        type_instance = type_parts[1].strip()
        if (type_class not in VALIDATED_MIME_TYPES.keys()
            or type_instance not in VALIDATED_MIME_TYPES.get(type_class, [])):
            raise ContentLoadError('Unsupported file format', 
                                   http_code=415,
                                   app_code='/content/file_format/unsupported_mime_type', 
                                   mime_type=found_type)
            
        if self.media_type != found_type:
            raise ContentLoadError('Encountered unexpected media type',
                                   app_code='/content/file_format/mime_type_mismatch', 
                                   expected_mime_type=self.media_type, 
                                   detected_mime_type=found_type)

    def upload(self, mss, validateMimeType=False, use_permission_of=None,
               permission=None, required_content_id=None, license_id=None, 
               rights_holder=None):
        """
        upload the content to the clob.

        if the content is attached to a stream, the upload
        should stream too.
        """
        if not self._editable:
            raise ContentLoadError('Attempt to upload an immutable document',
                app_code='/content/upload/immutable_document',
                id = self.document_id)

        if required_content_id:
            assert self.document_id, "Need a document to reference content_id against"
            # when we read, make sure to set cache=False to guarantee
            # we've got the absolute latest
            # optimistically compare against the required content id since it's faster
            # than resolving the id if it's not there...  we can always make more queries
            # to whittle down the original cause of the error.
            query = {
                "id": self.document_id, 
                "/common/document/content": {"id": required_content_id}
            }

            if not mss.mqlread(query, cache=False):
                if not mss.mqlread({"id": self.document_id}):
                    raise ContentLoadError("Invalid document id",
                                           app_code="/content/upload/invalid_document",
                                           requested_content=required_content_id,
                                           requested_document=self.document_id)
                
                existing_content = mss.mqlread({
                        "id": None,
                        "!/common/document/content": {"id": self.document_id}
                })
                if not existing_content:
                    raise ContentLoadError("Requested a content node, but document has no content node",
                                           app_code="/content/upload/content_mismatch",
                                           requested_content=required_content_id,
                                           requested_document=self.document_id)
                else:
                    assert existing_content["id"] != required_content_id
                    raise ContentLoadError("Document content does not match",
                                           app_code='/content/upload/content_mismatch',
                                           requested_content=required_content_id,
                                           existing_content=existing_content["id"])
               
            
        # XXX access control error if we attempt to construct
        #  content without being /user/metaweb?
        user_id = mss.get_user_id()

        # if we want to generate wrappers (e.g. to get an
        # image's width and height) we have to load it into
        # memory.
        wrappers = self.wrappers()
        if self.body is None and len(wrappers) > 0:
            for wrapper in wrappers:
                wrapper.update_content()
            self.fetch_body(mss=mss)

        # currently the Blob code only handles one language
        if isinstance(self.language, LanguageCode):
            # XXX warning, this really should be a list
            lang_id = self.language.id
        elif isinstance(self.language, list) and len(self.language) > 0:
            lang_id = LanguageCode(self.language[0]).id
        else:
            # XXX really should have no default
            lang_id = '/lang/en'


        # Blob() wants a file stream...
        if self.body is not None and len(self.body):
            file = cStringIO.StringIO(self.body)
        else:
            file = self.file
    
        # only validate if asked to
        if validateMimeType:
            self.validate_file_type(file)

        #
        # this constructor uploads the bits from the file object
        #
        blob = Blob(mss,
                    file,
                    self.filename, # display name
                    lang_id,
                    self.content_type_header(),
                    content_length=self.length,
                    user=user_id, 
                    license_id=license_id,
                    rights_holder=rights_holder)
  
        self.blob_id = blob.key
        self.content_id = blob.guid
        self.length = blob.len
        
        # if we got this far, then the license/rights are good:
        self.license_id = blob.license_id
        self.rights_holder = blob.rights_holder

        return self._update_blob(mss, wrappers, 
                                 use_permission_of=use_permission_of, 
                                 use_permission=permission)

    def _update_blob(self, mss, wrappers, 
                     use_permission_of=None, use_permission=None):
        """
        Update any wrappers and create /common/document if necessary
        """
        if not self._editable:
            raise ContentLoadError('Attempt to upload an immutable document',
                                   app_code='/content/upload/immutable_document',
                                   id=self.document_id)
        # upload any import info
        self.upload_fetch_record(mss)
        
        # if we have type-specific info (e.g. image size), upload it.
        if wrappers:
            # we had better be in the graph already
            assert isinstance(self.content_id, basestring)
            # and loaded to boot
            assert self.body is not None

            # upload the wrapper-specific data
            # this requires parsing the content - should be optional?
            for wr in wrappers:
                wr.upload(mss)

        # now, if we have a document, write that link
        LOG.info("content", "Ok, thinking about uploading to '%s'" % self.document_id);
        if self.document_id is not None:
            doc_id = self.document_id
            if doc_id == NEW_DOCUMENT:
                doc_id = None
            now = datetime.utcnow().isoformat('T') + 'Z'
            d = {
                "id": doc_id,
                "type": "/common/document",
                "/common/document/content": {"id": self.content_id },
                "/common/document/updated": {"value": now}
            }

            permission_varenv = {}
            if doc_id:
                d['/common/document/content']['connect'] = 'update'
                d['/common/document/updated']['connect'] = 'update'
            else:
                d['create'] = 'unconditional'
                if use_permission:
                    permission_varenv['permission'] = use_permission
                elif use_permission_of:
                    # get permission for guid
                    pqr = mss.mqlread({"id": use_permission_of, "permission": None})
                    if not (pqr and pqr["permission"]):
                        raise ContentLoadError('Permission does not exists',
                                               app_code='/content/upload/invalid_permission',
                                               permission_of=use_permission_of)
                    LOG.notice("document", 
                               "Using permission %s of %s" % (pqr['permission'], use_permission_of))
                    permission_varenv['permission'] = pqr['permission']

            
            with mss.push_variables(user=mss.get_user().id, **permission_varenv):
                set_oauth_attribution_if_needed(mss)
                r = mss.mqlwrite(d)
                    
            if doc_id is None:
                self.document_id = r['id']

            LOG.notice("content", 
                       'linked content %s to document %s' % (self.content_id, self.document_id))

        return self.to_mql()

    def read(self, mss=None):
        """
        get a string containing the binary data for the content
        """
        if self.body is None:
            self.fetch_body(mss=mss)
        return self.body

    def to_mql(self):
        # XXX todo translate the media_type and text_encoding
        d = {"id": self.content_id, "type": '/type/content'}
        # this is ok as long as it it not used
        # by code that has not just completed 
        # a write
        
        fq_props = {
            '/type/content/blob_id': self.blob_id,
            '/type/content/media_type': self.media_type,
            '/type/content/text_encoding': self.text_encoding,
            '/type/content/length': self.length,
            '/type/content/language': self.language
        }
        d.update(fq_props)

        if self.license_id is not None:
            d['/common/licensed_object/license'] = self.license_id

        if self.rights_holder is not None:
            d['/common/licensed_object/rights_holder'] = self.rights_holder
            
        if self.document_id is not None and self.document_id != NEW_DOCUMENT:
            d['document'] = self.document_id
        return d
    
    def to_mqlref(self):
        assert self.content_id, 'Content has no id, may need upload()?'
        return {"id": self.content_id}

    def open(self):
        """
        get a filehandle for reading binary data from the content
        """
        raise NotImplementedError

    def deliver(self, environ, start_response):
        """
        wsgi interface to GET a content instance

        environ is ignored at the moment.
        """
        # use blob_id as ETag? interchangeably?


        # if we only have self.file, we should return a generator that reads
        #  from it instead of fetching the body
        
        mss = environ['mw.service_session']
        self.fetch_body(mss=mss)

        # should return content-language too
        
        headers = [('Content-Type', str(self.content_type_header())),
                   ('Content-Length', str(len(self.body)))]

        start_response('200 OK', headers)

        return (self.body,)

    # method to get a wsgi-style generator-of-strings?

    def wrappers(self):
        """
        return a list of type-specific wrappers for this
        content.
        """
        return ContentWrapper.make_wrappers(self)

class ContentWrapper(object):
    """
    a ContentWrapper subclass adds media type specific behavior
    to an instance of Content.

    Each subclass should have a static match(content) method which checks
    whether it applies to the provided content.
    
    """

    # global registry of available subclasses
    # currently set up in mw.wsgi.top
    subclasses = []

    @staticmethod
    def register(subcls):
        ContentWrapper.subclasses.append(subcls)
        LOG.info("content", 'registered content wrapper', subcls=subcls)

    @staticmethod
    def make_wrappers(c):
        """
        wrap a piece of content with all wrappers that seem to
        apply to it.
        """
        return [cls(c) for cls in ContentWrapper.subclasses if cls.match(c)]

    
    def __init__(self, content):
        self.content = content


    @staticmethod
    def match(content):
        """
        subclasses should override this method, returning True
        if the subclass can successfully wrap the content.
        """
        raise NotImplementedError

    def upload(self, mss):
        """
        subclasses should override this method to upload 
        wrapper-specific data to the graph.

        the /type/content must already exist in the graph
        and self.content.content_id must be valid.
        """
        return

    def update_content(self):
        pass


class TextContent(ContentWrapper):
    """
    base class for mw.formats.HTMLContent
    """

    @staticmethod
    def match(c):
        if c.media_type.startswith('text/'):
            return True
        return False

    # methods to present the blob as unicode
    def uread(self):
        """
        get a unicode object containing the unicode text for the content
        """
        raise NotImplementedError
    
    # basic blurb extractor

######################################################################
#
#   more formats...
#

class XMLContent(TextContent):
    """
    xml

    figure out text_encoding according to xml rules
    """

    def guess_encoding(self):
        # "this is so much trickier than it sounds, it's not even funny."
        headers = {}
        headers['content-type'] = self.content.content_type_header()
        # where is xmldata defined???
        xmldata = ''
        feedparser._getCharacterEncoding(headers, xmldata)

#ContentWrapper.register(XMLContent)

######################################################################
