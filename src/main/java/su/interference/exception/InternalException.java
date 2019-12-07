/**
 The MIT License (MIT)

 Copyright (c) 2010-2019 head systems, ltd

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

package su.interference.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.core.Instance;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class InternalException extends Exception {

    private Exception e;
    private final static Logger logger = LoggerFactory.getLogger(InternalException.class);

    public InternalException (Exception e) {
        super();
        this.e = e;
        checkDebugMode();
    }

    public InternalException () {
        super();
        this.e = this;
        checkDebugMode();
    }

    public String getLogMessage () {
        StringBuffer r = new StringBuffer();
        StackTraceElement[] ste = this.e.getStackTrace();
        for (int i=0; i<ste.length; i++) {
            r.append(ste[i].getClassName());
            r.append(".");
            r.append(ste[i].getMethodName());
            r.append(" line ");
            r.append(ste[i].getLineNumber());
            r.append(" throws ");
            r.append(this.e.getClass().getSimpleName());
        }
        return r.toString();
    }

    private void checkDebugMode() {
        if (Instance.LOG_INTERNAL_EXCEPTIONS) {
            logger.info(getLogMessage());
        }
    }
}
