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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final static Logger logger = LoggerFactory.getLogger(LLT.class);
    private final boolean lock;
    private final long id;
    private final StackTraceElement[] trace;

    // WARNING!!!
    // change of 'debug' value to true causes decrease total performance
    // dev & QA engineers may change this constant
    private static final boolean debug = false;

    private LLT(long id, boolean lock) {
        this.id = id;
        this.lock = lock;
        this.trace = debug ? Thread.currentThread().getStackTrace() : null;
    }

    public static long getSyncId() {
        return sync.get();
    }

    public static LLT getLLT() throws InterruptedException {
        final long id_ = Thread.currentThread().getId();
        if (pool.get(id_) != null) {
            if (debug) {
                for (StackTraceElement e : pool.get(id_).getTrace()) {
                    logger.info(e.toString());
                }
            }
            logger.error("an unexpected attempt to get llt with id = "+id_+" which already exists");
            throw new RuntimeException("an unexpected attempt to get llt with id = "+id_+" which already exists");
        }
        rlck.lock();
        final LLT llt = new LLT(id_, false);
        pool.put(llt.getId(), llt);
        rlck.unlock();
        return llt;
    }

    public static LLT getLLTAndLock() throws InterruptedException {
        final long id_ = Thread.currentThread().getId();
        if (pool.get(id_) != null) {
            logger.error("an unexpected attempt to get llt with id = "+id_+" which already exists");
            throw new RuntimeException("an unexpected attempt to get llt with id = "+id_+" which already exists");
        }
        if (Config.getConfig().SYNC_LOCK_ENABLE) {
            rlck.lock();
        }
        while(poolNotEmpty()) { }
        final LLT llt = new LLT(id_, true);
        sync.compareAndSet(0, llt.getId());
        pool.put(llt.getId(), llt);
        return llt;
    }

    private static boolean poolNotEmpty() {
        final int size = pool.size();
        return size!=0;
    }

    public void add(Frame b) {
        b.getFrameData().setSynced(false);
        frames.put(b.getFrameData().getFrameId(), b);
    }

    public void commit() {
        pool.remove(this.id);
        if (this.lock) {
            for (Map.Entry<Long, Frame> entry : frames.entrySet()) {
                entry.getValue().getFrameData().setSynced(true);
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

    private StackTraceElement[] getTrace() {
        return trace;
    }
}
