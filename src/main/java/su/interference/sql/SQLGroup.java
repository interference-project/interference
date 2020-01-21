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

package su.interference.sql;

import su.interference.core.DataChunk;
import su.interference.exception.InternalException;

import java.util.*;
import java.net.MalformedURLException;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SQLGroup {

    private final DataChunk dc;
    private final List<SQLColumn> cols;
    private final SQLGroupFunction[] fset;

    public SQLGroup (DataChunk c, List<SQLColumn> cols) {
        this.dc   = c;
        this.cols = cols;
        this.fset = new SQLGroupFunction[cols.size()];
        for (int i=0; i<this.cols.size(); i++) {
            if (this.cols.get(i).getFtype()>0) {
                fset[i] = new SQLGroupFunction(this.cols.get(i).getFtype());
            }
        }
    }

    public void add (DataChunk c) throws InternalException {
        Object[] os = c.getDcs().getValueSet();
        if (os.length!=this.cols.size()) { throw new InternalException(); }
        for (int i=0; i<this.cols.size(); i++) {
            if (this.cols.get(i).getFtype()>0) {
                fset[i].add(os[i]);
            }
        }
    }

    public DataChunk getDC() {
        for (int i=0; i<this.cols.size(); i++) {
            if (this.cols.get(i).getFtype()>0) {
                this.dc.getDcs().getValueSet()[i] = fset[i].getResult();
            }
        }
        return this.dc;
    }

}
