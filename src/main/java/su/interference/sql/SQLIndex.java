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

import su.interference.core.ValueSet;
import su.interference.exception.InternalException;
import su.interference.persistent.FrameData;
import su.interference.persistent.Session;
import su.interference.persistent.Table;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SQLIndex implements FrameIterator, Finder {
    private final Table t;
    private final Table parent;
    private final SQLColumn lkey;
    private final SQLColumn rkey;
    private final boolean left;
    private final boolean unique;
    private final boolean merged;
    private final List<FrameData> frames;
    private int   amount;
    private int   cntrStart;
    private final AtomicInteger framecntr;
    private final AtomicBoolean returned;
    private final ValueCondition vc;

    public SQLIndex(Table t, Table parent, boolean left, SQLColumn lkey, SQLColumn rkey, boolean merged, NestedCondition nc, Session s) throws InternalException, IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        if (!t.isIndex()) throw new InternalException();
        this.t = t;
        this.lkey = lkey;
        this.rkey = rkey;
        this.parent = parent;
        this.left = left;
        this.unique = left?lkey.isUnique():rkey.isUnique();
        this.merged = merged;
        frames = t.getFrames(s);
        amount = frames.size();
        cntrStart = 0;
        this.framecntr = new AtomicInteger(0);
        this.returned = new AtomicBoolean(false);
        this.vc = nc.getIndexVC(this, t);
    }

    public List<Object> get(Object key) throws Exception {
        List<Object> res = new ArrayList<>();
        if (!left&&unique) {
            res.add(t.getObjectByKey(new ValueSet(key)));
            return res;
        } else if (!left) {
            res.addAll(t.getObjectsByKey(new ValueSet(key)));
            return res;
        }
        return null;
    }

    public FrameApi nextFrame() throws InternalException, ClassNotFoundException, MalformedURLException {
        if (!merged||left) {
            if (hasNextFrame()) {
                FrameData bd = frames.get(framecntr.get());
                framecntr.incrementAndGet();
                return new SQLIndexFrame(t, parent, bd, lkey, rkey, vc, left, unique, merged);
            }
        } else {
            if (merged&&!returned.get()) {
                returned.compareAndSet(false, true);
                return new SQLIndexFrame(t, parent, null, lkey, rkey, vc, left, unique, merged);
            }
        }
        return null;
    }

    public void resetIterator() {
        if (!merged||left) {
            if (!hasNextFrame()) {
                framecntr.set(0);
            }
        }
        if (merged&&returned.get()) {
            returned.compareAndSet(true, false);
        }
    }

    public boolean hasNextFrame() {
        if (!merged||left) {
            if ((this.amount + this.cntrStart) > framecntr.get()) {
                return true;
            }
        } else {
            if (merged&&!returned.get()) {
                return true;
            }
        }
        return false;
    }

    public int getType() {
        return FrameIterator.TYPE_TABLE;
    }

    public boolean isIndex() throws MalformedURLException, ClassNotFoundException {
        return t.isIndex();
    }

    public int getObjectId() {
        return parent.getObjectId();
    }

    public List<Integer> getObjectIds() {
        return Arrays.asList(new Integer[]{parent.getObjectId()});
    }

    @Override
    public boolean isLeftfs() {
        return false;
    }

    @Override
    public void setLeftfs(boolean leftfs) {

    }

}
