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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class LLT {

    private static final AtomicLong cntr = new AtomicLong();
    private static final AtomicLong sync = new AtomicLong();
    private static final ReentrantLock rlck = new ReentrantLock();
    private static final ConcurrentHashMap<Long, LLT> pool = new ConcurrentHashMap<Long, LLT>();
    private static final ConcurrentHashMap<Long, Frame> frames = new ConcurrentHashMap<Long, Frame>();
    private final boolean lock;
    private final long id;

    private LLT(boolean lock) {
        id = cntr.incrementAndGet();
        this.lock = lock;
    }

    public static long getSyncId() {
        return sync.get();
    }

    public static LLT getLLT() throws InterruptedException {
        rlck.lock();
        final LLT llt = new LLT(false);
        pool.put(llt.getId(), llt);
        rlck.unlock();
        return llt;
    }

    public static LLT getLLTAndLock() throws InterruptedException {
        if (Config.getConfig().SYNC_LOCK_ENABLE) {
            rlck.lock();
        }
        while(poolNotEmpty()) { }
        final LLT llt = new LLT(true);
        sync.compareAndSet(0, llt.getId());
        pool.put(llt.getId(), llt);
        return llt;
    }

    private static boolean poolNotEmpty() {
        final int size = pool.size();
        return size!=0;
    }

    public void add(Frame b) {
        frames.put(b.getFrameData().getFrameId(), b);
    }

    public void commit() {
        pool.remove(this.id);
        if (this.lock) {
            for (Map.Entry<Long, Frame> entry : frames.entrySet()) {
                entry.getValue().clearSnaps(this.id);
            }
            frames.clear();
            sync.compareAndSet(this.id, 0);
            if (Config.getConfig().SYNC_LOCK_ENABLE) {
                rlck.unlock();
            }
        }
    }

    public static ConcurrentHashMap<Long, Frame> getFrames() {
        return frames;
    }

    public long getId() {
        return id;
    }

}
