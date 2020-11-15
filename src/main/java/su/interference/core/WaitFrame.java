/**
 The MIT License (MIT)

 Copyright (c) 2010-2020 head systems, ltd

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

import su.interference.persistent.FrameData;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class WaitFrame {

    private volatile FrameData bd;
    private final AtomicLong busy;

    public WaitFrame() {
        this.busy = new AtomicLong(0);
    }

    public WaitFrame(FrameData bd) {
        this.bd = bd;
        this.busy = new AtomicLong(0);
    }

    public synchronized WaitFrame acquire(boolean force) {
        if (this.bd == null) {
            return null;
        }
        if (force) {
            this.busy.set(Thread.currentThread().getId());
            return this;
        }
        if (this.busy.compareAndSet(0, Thread.currentThread().getId())) {
            return this;
        }
        return null;
    }

    public synchronized WaitFrame acquire(final int fileId, boolean force) {
        if (this.bd == null) {
            return null;
        }
        if (this.bd.getFile() == fileId) {
            if (force) {
                this.busy.set(Thread.currentThread().getId());
                return this;
            }
            if (this.busy.compareAndSet(0, Thread.currentThread().getId()) || this.busy.compareAndSet(Thread.currentThread().getId(), Thread.currentThread().getId())) {
                return this;
            }
        }
        return null;
    }

    public synchronized boolean trySetBd(FrameData oldbd, FrameData newbd, int frameType) {
        if (oldbd==null) {
            if (this.bd == null) {
                this.bd = newbd;
                return true;
            }
        } else if ((this.bd.getFile() == oldbd.getFile()&&(busy.get() == Thread.currentThread().getId()))||frameType > 0) {
            this.bd = newbd;
            return true;
        }
        return false;
    }

    public synchronized boolean trySetBdAndAcquire(FrameData bd) {
        if (this.busy.compareAndSet(0, Thread.currentThread().getId())) {
            if (this.bd == null) {
                this.bd = bd;
                return true;
            } else {
                this.busy.compareAndSet(Thread.currentThread().getId(), 0);
                return false;
            }
         }
        return false;
    }

    public FrameData getBd() {
        return bd;
    }

    public AtomicLong getBusy() {
        return busy;
    }

    public void release() {
        //this.busy.compareAndSet(Thread.currentThread().getId(), 0);
        this.busy.set(0);
    }

}
