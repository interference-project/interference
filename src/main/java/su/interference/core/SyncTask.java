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

import su.interference.metrics.Metrics;
import su.interference.persistent.DataFile;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Callable;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SyncTask implements Callable<Integer> {

    private final PriorityBlockingQueue<SyncFrame> pq = new PriorityBlockingQueue();
    private final DataFile df;

    public SyncTask(DataFile df) {
        this.df = df;
    }

    public void add(SyncFrame bd) {
        pq.add(bd);
    }

    public DataFile getDataFile() {
        return df;
    }

    public Integer call() {
        Metrics.get("syncFrames").start();
        try {

            while (pq.peek() != null) {
                final SyncFrame bd = pq.poll();
                final long ptr = bd.getFrameId() - (bd.getFrameId()%4096);
                this.df.writeFrame(ptr, bd.getBytes());
                //Storage.getStorage().writeFrameWithBackup(bd.getBytes(), bd.getFrameId());
            }

            Metrics.get("syncFrames").stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

}
