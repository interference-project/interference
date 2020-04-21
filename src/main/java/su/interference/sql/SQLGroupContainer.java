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
import su.interference.core.ValueSet;
import su.interference.exception.InternalException;
import su.interference.persistent.Session;
import su.interference.persistent.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SQLGroupContainer {

    private DataChunk dc;
    private final List<SQLColumn> cols;
    private final List<SQLColumn> gcols;
    private SQLColumn wcolumn;
    private int wcolumnInterval;
    private final SQLGroupFunction[] fset;
    private final List<DataChunk> w_ = new ArrayList<>();

    public SQLGroupContainer(List<SQLColumn> cols, List<SQLColumn> gcols) {
        this.cols = cols;
        this.gcols = gcols;
        this.fset = new SQLGroupFunction[cols.size()];
        for (int i=0; i<this.cols.size(); i++) {
            if (this.cols.get(i).getFtype()>0) {
                fset[i] = new SQLGroupFunction(this.cols.get(i).getFtype());
            }
        }
        for (SQLColumn gcolumn : cols) {
            if (gcolumn.isWindow()) {
               wcolumn = gcolumn;
               wcolumnInterval = gcolumn.getWindowInterval();
            }
        }
    }

    public DataChunk add (DataChunk c, Table target, Session s) throws InternalException {
        if (wcolumn != null) {
            return add2Window(c, target, s);
        }
        if (dc != null) {
            if (c != null && dc.compare(c, gcols.size()) == 0) {
                final Object[] os = c.getDcs().getValueSet();
                if (os.length != this.cols.size()) {
                    throw new InternalException();
                }
                for (int i = 0; i < this.cols.size(); i++) {
                    if (this.cols.get(i).getFtype() > 0) {
                        fset[i].add(os[i]);
                    }
                }
                return null;
            } else {
                final ValueSet groupvs = this.dc.getDcs();
                for (int i=0; i<this.cols.size(); i++) {
                    if (this.cols.get(i).getFtype()>0) {
                        groupvs.getValueSet()[i] = fset[i].getResult();
                        fset[i].clear();
                    }
                }
                final DataChunk res = new DataChunk(groupvs, s, target);
                this.dc = c;
                final Object[] os = c.getDcs().getValueSet();
                if (os.length != this.cols.size()) {
                    throw new InternalException();
                }
                for (int i = 0; i < this.cols.size(); i++) {
                    if (this.cols.get(i).getFtype() > 0) {
                        fset[i].add(os[i]);
                    }
                }
                return res;
            }
        } else {
            this.dc = c;
            final Object[] os = c.getDcs().getValueSet();
            if (os.length != this.cols.size()) {
                throw new InternalException();
            }
            for (int i = 0; i < this.cols.size(); i++) {
                if (this.cols.get(i).getFtype() > 0) {
                    fset[i].add(os[i]);
                }
            }
            return null;
        }
    }

    private DataChunk add2Window(DataChunk c, Table target, Session s) throws InternalException {
        if (c != null) {
            w_.add(c);
        }
        if (w_.size() < wcolumnInterval) {
            return null;
        } else {
            if (w_.size() > wcolumnInterval) {
                w_.remove(0);
            }
            if (w_.size() != wcolumnInterval) {
                throw new InternalException();
            }
            if (dc == null) {
                dc = w_.get(0);
            }
            for (DataChunk c_ : w_) {
                final Object[] os = c_.getDcs().getValueSet();
                if (os.length != this.cols.size()) {
                    throw new InternalException();
                }
                for (int i = 0; i < this.cols.size(); i++) {
                    if (this.cols.get(i).getFtype() > 0) {
                        fset[i].add(os[i]);
                    }
                }
            }
            final ValueSet groupvs = this.dc.getDcs();
            for (int i=0; i<this.cols.size(); i++) {
                if (this.cols.get(i).getFtype()>0) {
                    groupvs.getValueSet()[i] = fset[i].getResult();
                    fset[i].clear();
                }
            }
            final DataChunk res = new DataChunk(groupvs, s, target);
            this.dc = null;
            return res;
        }
    }

}
