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

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class ChunkMap {
    private final ConcurrentHashMap<Integer, Chunk> hmap;
    private final ConcurrentHashMap<ValueSet, List<Chunk>> imap;
    private final List<Chunk> list;
    private final Frame frame;
    private volatile boolean sorted;

    public ChunkMap(Frame frame) {
        hmap = new ConcurrentHashMap<>();
        imap = new ConcurrentHashMap<>();
        list = new CopyOnWriteArrayList<>();
        this.frame = frame;
    }

    public synchronized void sort() {
        Collections.sort(list);
        sorted = true;
    }

    public synchronized void add(Chunk c) {
        hmap.put(c.getHeader().getPtr(), c);
        if (frame instanceof IndexFrame) {
            if (imap.get(c.getDcs()) == null) {
                imap.put(c.getDcs(), new ArrayList<>());
            }
            imap.get(c.getDcs()).add(c);
        }
        list.add(c);
        sorted = false;
    }

    public synchronized List<Chunk> getChunks() {
        return list;
    }

    public synchronized Chunk getByPtr(int i) {
        return hmap.get(i);
    }

    public synchronized Chunk get(int i) {
        return list.get(i);
    }

    //for unique indexes
    public synchronized List<Chunk> getByKey(ValueSet key) {
        return imap.get(key);
    }

    public synchronized void removeByPtr(int i) {
        final boolean x = list.remove(hmap.get(i));
        final Chunk c = (Chunk)hmap.remove(i);
        if (frame instanceof IndexFrame) {
            int i_ = 0;
            for (int i__ = 0; i__ <  imap.get(c.getDcs()).size(); i__++) {
                if (imap.get(c.getDcs()).get(i__).getHeader().getPtr() == c.getHeader().getPtr()) {
                    i_ = i__;
                }
            }
            imap.get(c.getDcs()).remove(i_);
        }
        sorted = false;
        if (!x || c == null) {
            throw new RuntimeException("Internal error during remove object from frame");
        }
    }

    public synchronized void remove(int i) {
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
        sorted = false;
    }

    public synchronized int size() {
        return list.size();
    }

    public synchronized void clear() {
        hmap.clear();
        list.clear();
        imap.clear();
        sorted = false;
    }

    public synchronized boolean isSorted() {
        return sorted;
    }
}
