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

import su.interference.core.IndexChunk;
import su.interference.exception.InternalException;
import su.interference.persistent.Session;
import su.interference.persistent.Table;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.util.ArrayList;
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
    private final AtomicBoolean complete;
    private final AtomicBoolean returned;
    private final Table t;
    private final Class c;
    private final Session s;

    public SQLHashMap(SQLColumn cmap, SQLColumn ckey, FrameIterator rbi, Table t, Session s) throws MalformedURLException, ClassNotFoundException {
        this.cmap = cmap;
        this.ckey = ckey;
        this.rbi = rbi;
        this.complete = new AtomicBoolean(false);
        this.returned = new AtomicBoolean(false);
        this.t = t;
        this.c = t.getTableClass();
        this.s = s;
    }

    public Comparable getKeyValue(Class c, Object o, SQLColumn sqlc, Session s) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method y = c.getMethod("get"+sqlc.getColumn().getName().substring(0,1).toUpperCase()+sqlc.getColumn().getName().substring(1,sqlc.getColumn().getName().length()), new Class<?>[]{Session.class});
        return (Comparable)y.invoke(o, new Object[]{s});
    }

    public FrameApi nextFrame() throws Exception {
        if (!complete.get()) {
            while (rbi.hasNextFrame()) {
                FrameApi bd = rbi.nextFrame();
                if (bd != null) {
                    ArrayList<Object> drs = bd.getFrameEntities(s);
                    for (Object o : drs) {
                        if (bd.getImpl() == FrameApi.IMPL_INDEX) {
                            final IndexChunk ib1 = (IndexChunk) o;
                            o = ib1.getDataChunk().getEntity();
                        }

                        hmap.put(getKeyValue(c, o, cmap, s), o);
                    }
                }
            }
            complete.compareAndSet(false, true);
        }
        if (!returned.get()) {
            returned.compareAndSet(false, true);
            return new SQLHashMapFrame(hmap, cmap, ckey, t);
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

    public boolean isIndex() throws MalformedURLException, ClassNotFoundException {
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

}
