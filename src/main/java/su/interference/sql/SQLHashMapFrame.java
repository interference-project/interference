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
import su.interference.core.DataChunk;
import su.interference.core.IndexChunk;
import su.interference.persistent.Session;
import su.interference.persistent.Table;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SQLHashMapFrame implements FrameApi, Finder {
    private final ConcurrentHashMap<Comparable, Object> hmap;
    private final SQLColumn cmap;
    private final SQLColumn ckey;
    private final Table t;
    private final SQLIndexFrame ix;

    public SQLHashMapFrame(FrameIterator rbi, SQLColumn cmap, SQLColumn ckey, Table t, Class c, Session s) throws Exception {
        this.hmap = new ConcurrentHashMap<>();
        this.cmap = cmap;
        this.ckey = ckey;
        this.t = t;
        if (rbi instanceof SQLIndex) {
            if (rbi.hasNextFrame()) {
                FrameApi fa = rbi.nextFrame();
                if (fa instanceof SQLIndexFrame) {
                    ix = (SQLIndexFrame) fa;
                } else {
                    throw new RuntimeException("internal issue occured during sql processing");
                }
            } else {
                throw new RuntimeException("internal issue occured during sql processing");
            }
        } else {
            ix = null;
            while (rbi.hasNextFrame()) {
                FrameApi bd = rbi.nextFrame();
                if (bd != null) {
                    ArrayList<Object> drs = bd.getFrameEntities(s);
                    for (Object o : drs) {
                        hmap.put(getKeyValue(c, o, cmap, s), o);
                    }
                }
            }
        }
    }

    public int getImpl() {
        return FrameApi.IMPL_HASH;
    }

    public List<Object> get(Object key, Session s) throws Exception {
        Object o = hmap.get(key);
        if (o == null && ix != null) {
            List<Object> l = ix.get(key, s);
            if (l != null) {
                for (Object o_ : l) {
                    if (o_ != null) {
                        Object e = ((IndexChunk) ((DataChunk) o_).getEntity()).getDataChunk().getEntity();
                        hmap.put((Comparable) key, e);
                        return Arrays.asList(new Object[]{e});
                    }
                }
            }
        }
        return o == null ? null : Arrays.asList(new Object[]{o});
    }

    @SuppressWarnings("unchecked")
    public Comparable getKeyValue(Class c, Object o, SQLColumn sqlc, Session s) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method y = c.getMethod("get"+sqlc.getColumn().getName().substring(0,1).toUpperCase()+sqlc.getColumn().getName().substring(1,sqlc.getColumn().getName().length()), null);
        return (Comparable)y.invoke(o, null);
    }


    public SQLColumn getCmap() {
        return cmap;
    }

    public SQLColumn getCkey() {
        return ckey;
    }

    public long getFrameId() {
        return 0;
    }

    public long getFrameOrder() {
        return 0;
    }

    public long getAllocId() {
        return 0;
    }

    public int getObjectId() {
        return t.getObjectId();
    }

    public ArrayList<Chunk> getFrameChunks(Session s) {
        return null;
    }

    public ArrayList<Object> getFrameEntities(Session s) {
        return null;
    }
}
