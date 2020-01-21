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

import su.interference.core.Chunk;
import su.interference.exception.InternalException;
import su.interference.persistent.Session;
import su.interference.persistent.Table;

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

    public SQLHashMapFrame(ConcurrentHashMap<Comparable, Object> hmap, SQLColumn cmap, SQLColumn ckey, Table t) {
        this.hmap = hmap;
        this.cmap = cmap;
        this.ckey = ckey;
        this.t = t;
    }

    public int getImpl() {
        return FrameApi.IMPL_HASH;
    }

    public List<Object> get(Object key) {
        return Arrays.asList(new Object[]{hmap.get(key)});
    }

//    public void put(Comparable k, Object v) {
//        hmap.put(k, v);
//    }

    public boolean hasLiveTransaction(long transId) {
        return false;
    }

    public boolean hasLocalTransactions() {
        return false;
    }

    public int hasRemoteTransactions() throws InternalException {
        return 0;
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
