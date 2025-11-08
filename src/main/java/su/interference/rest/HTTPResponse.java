/**
The MIT License (MIT)

Copyright (c) 2010-2025 interference

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 */

package su.interference.rest;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.core.Instance;

import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

public class HTTPResponse {

    public final String status;
    public final String mimeType;
    public final InputStream data;
    private Properties header = new Properties();
    private final static Logger logger = LoggerFactory.getLogger(HTTPResponse.class);

    public HTTPResponse() {
        this.status = HTTPSession.HTTP_200_OK;
        this.mimeType = null;
        this.data = null;
    }

    public HTTPResponse(String status, String mimeType, InputStream data) {
        this.status = status;
        this.mimeType = mimeType;
        this.data = data;
    }

    public HTTPResponse(String status, String mimeType, String s) {
        this.status = status;
        this.mimeType = mimeType;
        String cp;
        try {
            cp = Instance.getInstance().getCodePage();
        } catch (Exception e) {
            cp = "UTF-8";
        }
        InputStream data_ = null;
        try {
            data_ = new ByteArrayInputStream(s.getBytes(cp));
        } catch (UnsupportedEncodingException e) {
            logger.error(e.getMessage(), e);
        }
        this.data = data_;
    }

    public void addHeader (String name, String value) {
        header.put(name, value);
    }

    public Properties getHeader() {
        return header;
    }
}

