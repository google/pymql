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

functions for manipulating image content

everything is done in memory, we assume images
aren't too large.

"""

import os, contenttype
from StringIO import StringIO
from mw.log import LOG

from mw.api.content import Content, ContentWrapper
from mw.error import ContentLoadError
import mw.siteconfig
from mw.mql import scope

TN_MODES = ['fit', 'fill', 'fillcrop', 'fillcropmid']
DEF_TN_MODE = 'fit'

class ImageContent(ContentWrapper):
    """
    methods for dealing with image content
    """

    # ie6 uses some bizarre content_types for PNG and JPEG images
    # XXX it would be nice to fix the content_type in the
    #  /type/content object, but it may already have been uploaded.
    # so for now, images uploaded from ie6 will have the "wrong"
    #  content-type and we'll need to garden them.
    remap_dumb_ie_mime_types = {
        'image/pjpeg': contenttype.MediaType('image/jpeg'),
        'image/x-png': contenttype.MediaType('image/png')
    }


    @classmethod
    def match(cls, c):
        """
        true if this ContentWrapper subclass applies to the content argument.
        """
        media_type = cls.remap_dumb_ie_mime_types.get(c.media_type, c.media_type)
        if not c.media_type.startswith('image/'):
            return False

        subtype = media_type.split('/')[1]
        
        return subtype in ('gif', 'png', 'jpeg', 'x-icon')

    def __init__(self, content):
        super(ImageContent, self).__init__(content)
        self.size = None

    def load(self, mss):
        result = mss.mqlread(dict(id=self.content.content_id,
                             type='/common/image',
                             size=dict(x=None, y=None)))

        if result is None:
            return
        
        self.size = (result['size']['x'], result['size']['y'])

    def upload(self, mss):
        """
        add a /common/image facet to the type/content
        """
        self.load(mss)
        if self.size is None:
            self.parse(mss)
            
        w = { 'id': self.content.content_id,
              'type': { 'connect': 'insert',
                        'id': '/common/image' }}
        if self.size[0] and self.size[1]:
            w['/common/image/size'] = { 'create': 'unless_exists',
                                        'type': '/measurement_unit/rect_size',
                                        'x': self.size[0],
                                        'y': self.size[1] }

        with mss.push_variables(authority="/user/content_administrator",
                                privileged=scope.Authority):
            result = mss.mqlwrite(w)

    def parse(self, mss):
        """
        extract data from the image

        exif tags from digital cameras
        """
        # exif tags from digital cameras?
        
        self.content.fetch_body(mss)
        try:
            # XXXarielb move to pygmy as soon as pygmy doesn't crash within threads
            from PIL import Image
            img = Image.open(StringIO(self.content.body))
            # believe the image parser over anything in the graph
            self.size = img.size
        except ImportError, ie:
            LOG.error("format.image.no_pil", str(e))
            raise
        except Exception, e: 
            LOG.error("format.image.parse", str(e))
            raise ContentLoadError('Invalid image file', 
                                   app_code="upload/invalid_image_data", 
                                   error=e)

    def update_content(self):
        media_type = self.content.media_type
        LOG.info('update_content', "Image Updating content from %s to %s" % (media_type,
                                                      self.remap_dumb_ie_mime_types.get(media_type)))
        self.content.media_type = self.remap_dumb_ie_mime_types.get(media_type, media_type)
  
    @classmethod
    def get_fallback_image_path(cls):
	try:
            config = mw.siteconfig.get_config2()
            path = config.get('me.img_thumb_fallback')
            if path and os.path.exists(path):
                return path
	except KeyError, e:
	    pass

        LOG.error("image.thumb", "Could not find fallback image for thumbnailing service.")
        return None


    # failover for thumnailing operation in the event that 
    # the image is too large to thumbnail
    def thumb_fallback(self, mss):
        path = ImageContent.get_fallback_image_path()
        if path is None:
            return None
        # load data 
        fd = open(path)
        data = fd.read()
        fd.close()
        # the fallback image is a known GIF image.
        thumb_mt = 'image/gif'
        c = Content(media_type=thumb_mt)
        c.set_body(data)
        return c
