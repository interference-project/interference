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

import su.interference.core.Chunk;
import su.interference.core.RetrieveQueue;
import su.interference.core.ValueSet;
import su.interference.exception.InternalException;
import su.interference.persistent.FrameData;
import su.interference.persistent.Session;
import su.interference.persistent.Table;

import java.util.*;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SQLIndexFrame implements FrameApi, Finder {
    private final Table t;
    private final Table parent;
    private final FrameData bd;
    private final SQLColumn lkey;
    private final SQLColumn rkey;
    private final boolean left;
    private final boolean unique;
    private final boolean merged;
    private final boolean vccheck;
    private final int join;
    private RetrieveQueue rqueue;
    private final ValueCondition vc;
    private final Map<ValueSet, Object> vcmap;

    public SQLIndexFrame(Table t, Table parent, FrameData bd, SQLColumn lkey, SQLColumn rkey, ValueCondition vc, boolean left, boolean unique, boolean merged, int join, Session s)
            throws Exception {
        if (!t.isIndex()) {
            throw new InternalException();
        }
        this.t = t;
        this.parent = parent;
        this.bd = bd;
        this.lkey = lkey;
        this.rkey = rkey;
        this.left = left;
        this.vc = vc;
        this.unique = unique;
        this.merged = merged;
        this.join = join;
        this.vcmap = new HashMap<>();
        if (vc != null && (vc.getCondition() == Condition.C_EQUAL || vc.getCondition() == Condition.C_IN)) {
            this.vccheck = true;
            for (Object o : vc.getValues()) {
                final ValueSet vs = new ValueSet(o);
                for (Chunk c : t.getObjectsByKey(vs, s)) {
                    vcmap.put(vs, c.getEntity(s));
                }
            }
        } else {
            vccheck = false;
        }
    }

    public int getImpl() {
        return FrameApi.IMPL_INDEX;
    }

    public List<Object> get(Object key, Session s) throws Exception {
        if (vccheck) {
            return Arrays.asList(vcmap.get(new ValueSet(key)));
        }
        List<Object> res = new ArrayList<>();
        if (unique) {
            Object o = t.getObjectByKey(new ValueSet(key), s);
            if (o != null) {
                res.add(o);
            }
        } else {
            res.addAll(t.getObjectsByKey(new ValueSet(key), s));
        }
        return res;
    }

    public long getFrameId() {
        return bd == null ? 0 : bd.getFrameId();
    }

    public long getFrameOrder() {
        return bd == null ? 0 : bd.getFrameOrder();
    }

    public long getAllocId() {
        return bd == null ? 0 : bd.getAllocId();
    }

    public int getObjectId() {
        return parent.getObjectId();
    }

    public ArrayList<Chunk> getFrameChunks(Session s) {
        return null;
    }

    public ArrayList<Object> getFrameEntities(Session s) throws Exception {
        //todo need refactor on SQLIndex level - returns one to many frames by value
        synchronized (this) {
            if (vccheck) {
                ArrayList<Object> res = new ArrayList<>();
                for (Object o : vc.getValues()) {
                    for (Chunk c : t.getObjectsByKey(new ValueSet(o), s)) {
                        res.add(c.getEntity(s));
                    }
                }
                return res;
            } else {
                return bd.getFrameEntities(s);
            }
        }
    }

    public Object poll(Session s) {
        if (!left || join == SQLJoinDispatcher.MERGE) {
            if (rqueue == null) {
                rqueue = s.getContentQueue(t);
            }
            return rqueue.poll(s);
        } else {
            throw new RuntimeException("Wrong issue of internal join mechanism occured");
        }
    }

    public SQLColumn getLkey() {
        return lkey;
    }

    public SQLColumn getRkey() {
        return rkey;
    }

    public boolean isLeft() {
        return left;
    }

    public boolean isUnique() {
        return unique;
    }

    public boolean isMerged() {
        return merged;
    }

}
