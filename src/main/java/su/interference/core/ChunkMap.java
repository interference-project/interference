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

package su.interference.core;

import java.util.*;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class ChunkMap {
    private final HashMap<Integer, Chunk> hmap;
    private final HashMap<ValueSet, List<Chunk>> imap;
    private final List<Chunk> list;
    private final Frame frame;
    private volatile boolean sorted;
    private volatile int used;

    public ChunkMap(Frame frame) {
        hmap = new HashMap<>();
        imap = frame instanceof IndexFrame ? new HashMap<>() : null;
        list = frame instanceof IndexFrame ? new ArrayList<>() : null;
        this.frame = frame;
    }

    @SuppressWarnings("unchecked")
    protected synchronized void sort() {
        if (frame instanceof IndexFrame) {
            Collections.sort(list);
            sorted = true;
        }
    }

    protected synchronized void add(Chunk c) {
        hmap.put(c.getHeader().getPtr(), c);
        if (frame instanceof IndexFrame) {
            if (imap.get(c.getDcs()) == null) {
                imap.put(c.getDcs(), new ArrayList<>());
            }
            imap.get(c.getDcs()).add(c);
            list.add(c);
        }
        sorted = false;
        used = used + c.getBytesAmount();
    }

    protected synchronized void check() {
        for (Map.Entry<Integer, Chunk> entry : hmap.entrySet()) {
            Chunk c = entry.getValue();
            if (c.getHeader().getPtr() != entry.getKey()) {
                throw new RuntimeException("internal cmap check failed");
            }
        }
    }

    protected synchronized Collection<Chunk> getChunks() {
        if (frame instanceof IndexFrame) {
            return list;
        }
        return hmap.values();
    }

    protected synchronized Chunk getByPtr(int i) {
        return hmap.get(i);
    }

    // index only
    protected synchronized Chunk get(int i) {
        return list.get(i);
    }

    //for unique indexes
    protected synchronized List<Chunk> getByKey(ValueSet key) {
        return imap.get(key);
    }

    protected synchronized void removeByPtr(int i) {
        final Chunk c = hmap.remove(i);
        if (frame instanceof IndexFrame) {
            int i_ = 0;
            for (int i__ = 0; i__ <  imap.get(c.getDcs()).size(); i__++) {
                if (imap.get(c.getDcs()).get(i__).getHeader().getPtr() == c.getHeader().getPtr()) {
                    i_ = i__;
                }
            }
            imap.get(c.getDcs()).remove(i_);
            list.remove(hmap.get(i));
        }
        sorted = false;
        used = used - c.getBytesAmount();
        if (c == null) {
            throw new RuntimeException("Internal error during remove object from frame");
        }
    }

    // index only
    protected synchronized void remove(int i) {
        final Chunk c = list.get(i);
        list.remove(i);
        hmap.remove(c.getHeader().getPtr());
        if (frame instanceof IndexFrame) {
            int i_ = 0;
            for (int i__ = 0; i__ <  imap.get(c.getDcs()).size(); i__++) {
                if (imap.get(c.getDcs()).get(i__).getHeader().getPtr() == c.getHeader().getPtr()) {
                    i_ = i__;
                }
            }
            imap.get(c.getDcs()).remove(i_);
        }
        used = used - c.getBytesAmount();
        sorted = false;
    }

    //index only
    protected synchronized int size() {
        return list.size();
    }

    protected synchronized void clear() {
        hmap.clear();
        list.clear();
        imap.clear();
        used = 0;
        sorted = false;
    }

    protected synchronized boolean isSorted() {
        return sorted;
    }

    protected int getUsed() {
        return used;
    }

}
