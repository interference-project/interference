/**
 The MIT License (MIT)

 Copyright (c) 2010-2021 head systems, ltd

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

import su.interference.exception.InternalException;
import su.interference.persistent.Session;
import su.interference.persistent.Table;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SQLHashMap implements FrameIterator {
    private final ConcurrentHashMap<Comparable, Object> hmap = new ConcurrentHashMap<Comparable, Object>();
    private final SQLColumn cmap;
    private final SQLColumn ckey;
    private final FrameIterator rbi;
    private final AtomicBoolean returned;
    private final Table t;
    private final Class c;
    private final Session s;
    private SQLHashMapFrame hframe;

    public SQLHashMap(SQLColumn cmap, SQLColumn ckey, FrameIterator rbi, Table t, Session s) {
        this.cmap = cmap;
        this.ckey = ckey;
        this.rbi = rbi;
        this.returned = new AtomicBoolean(false);
        this.t = t;
        this.c = t.getTableClass();
        this.s = s;
    }

    public FrameApi nextFrame() throws Exception {
        if (!returned.get()) {
            synchronized (this) {
                if (hframe == null) {
                    hframe = new SQLHashMapFrame(rbi, cmap, ckey, t, c, s);
                }
                returned.compareAndSet(false, true);
                return hframe;
            }
        }
        return null;
    }

    public boolean hasNextFrame() throws InternalException {
        return !returned.get();
    }

    public void resetIterator() {
        returned.compareAndSet(true, false);
    }

    public int getType() {
        return FrameIterator.TYPE_TABLE;
    }

    public boolean isIndex() {
        return false;
    }

    public int getObjectId() {
        return t.getObjectId();
    }

    public List<Integer> getObjectIds() {
        return null;
    }

    @Override
    public boolean isLeftfs() {
        return false;
    }

    @Override
    public void setLeftfs(boolean leftfs) {

    }

    @Override
    public boolean noDistribute() {
        return false;
    }

}
