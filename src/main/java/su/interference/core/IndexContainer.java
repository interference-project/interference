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

import su.interference.persistent.FrameData;
import su.interference.persistent.Session;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class IndexContainer {
    private final LinkedBlockingQueue<FrameData> frameQueue;
    private final Session s;
    private volatile int cptr;
    private volatile List<Chunk> current;
    private volatile boolean started;
    private volatile boolean terminated;

    public IndexContainer(LinkedBlockingQueue<FrameData> frameQueue, Session s) {
        this.frameQueue = frameQueue;
        this.s = s;
        cptr = 0;
    }

    public Chunk next() throws Exception {
        if (terminated) {
            return null;
        }

        cptr++;

        if (current != null && current.size() == cptr) {
            FrameData current_ = frameQueue.take();
            cptr = 0;
            if (current_.getObjectId() == 0 && current_.getFrameId() == 0) {
                terminated = true;
                return null;
            }
            IndexFrame current__ = current_.getIndexFrame();
            current__.sort();
            current = current__.getFrameChunks(s);
        }

        return current.get(cptr);
    }

    public Chunk get() throws Exception {
        if (terminated) {
            return null;
        }

        if (!started) {
            FrameData current_ = frameQueue.take();
            started = true;

            if (current_.getObjectId() == 0 && current_.getFrameId() == 0) {
                terminated = true;
                return null;
            }

            IndexFrame current__ = current_.getIndexFrame();
            current__.sort();
            current = current__.getFrameChunks(s);
        }

        if (current.size() == 0) {
            return null;
        }

        return current.get(cptr);
    }

    public LinkedBlockingQueue<FrameData> getFrameQueue() {
        return frameQueue;
    }
}
