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
    private final ConcurrentHashMap<ValueSet, Chunk> imap;
    private final List<Chunk> list;
    private volatile boolean sorted;

    public ChunkMap() {
        hmap = new ConcurrentHashMap<>();
        imap = new ConcurrentHashMap<>();
        list = new CopyOnWriteArrayList<>();
    }

    public void sort() {
        Collections.sort(list);
        sorted = true;
    }

    public void add(Chunk c) {
        hmap.put(c.getHeader().getPtr(), c);
        imap.put(c.getDcs(), c);
        list.add(c);
        sorted = false;
    }

    public List<Chunk> getChunks() {
        return list;
    }

    public Chunk getByPtr(int i) {
        return hmap.get(i);
    }

    public Chunk get(int i) {
        return list.get(i);
    }

    public Chunk getByKey(ValueSet key) {
        return imap.get(key);
    }

    public void removeByPtr(int i) {
        final boolean x = list.remove(hmap.get(i));
        final Chunk c = (Chunk)hmap.remove(i);
        imap.remove(c.getDcs());
        sorted = false;
        if (!x || c == null) {
            throw new RuntimeException("Internal error during remove object from frame");
        }
    }

    public void remove(int i) {
        final Chunk c = list.get(i);
        list.remove(i);
        hmap.remove(c.getHeader().getPtr());
        imap.remove(c.getDcs());
        sorted = false;
    }

    public int size() {
        return list.size();
    }

    public void clear() {
        hmap.clear();
        list.clear();
        imap.clear();
        sorted = false;
    }

    public boolean isSorted() {
        return sorted;
    }
}
