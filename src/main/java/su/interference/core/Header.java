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

package su.interference.core;

import su.interference.persistent.Transaction;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public interface Header {

    public static final int RECORD_NORMAL_STATE  = 1;
    public static final int RECORD_LOCKED_STATE  = 2;
    public static final int RECORD_DELETED_STATE = 3;

    Transaction getTran ();
    int getState();
    int getLen();
    int getPtr();
    void setTran(Transaction tran);
    void setState(int state);
    void setLen(int len);
    void setPtr(int ptr);
    long getFramePtr();
    byte[] getHeader();
    RowId getRowID();
    void setRowID(RowId rowID);
    long getLltId();
    void setLltId(long lltId);

}
