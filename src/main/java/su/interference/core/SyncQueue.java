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

import java.util.concurrent.*;
import java.util.*;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.persistent.*;
import su.interference.exception.*;
import su.interference.sql.ContainerFrame;
import su.interference.sql.FrameApi;
import su.interference.sql.SQLCursor;
import su.interference.transport.TransportSyncTask;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SyncQueue implements Runnable, ManagedProcess {

    private volatile boolean f = true;
    private volatile boolean running = false;
    private final ExecutorService pool = Executors.newFixedThreadPool(Config.getConfig().FILES_AMOUNT);
    private final ExecutorService pool2 = Executors.newFixedThreadPool(1);
    CountDownLatch latch;
    private final static Logger logger = LoggerFactory.getLogger(SyncQueue.class);
    protected final static Object synclock = new Object();

    public SyncQueue() {
    }

    private synchronized boolean syncFramesFromQueue() throws Exception {
        synchronized (synclock) {
            if (running) {
                return false;
            }
            running = true;
            final LLT llt = LLT.getLLTAndLock();
            final int famt = Storage.getStorage().getFiles() == null ? 0 : Storage.getStorage().getFiles().size();
            logger.debug("sync procedure was started with frames amount=" + LLT.getFrames().size());

            final ArrayList<SyncFrame> frames = new ArrayList<>();
            final Map<Integer, List<FrameApi>> frames_ = new HashMap<>();
            final ArrayList<FreeFrame> fframes = new ArrayList<>();
            final Session s = Session.getDntmSession();

            for (Map.Entry<Long, FrameData> entry : LLT.getFrames().entrySet()) {
                FreeFrame fb = null;
                try {
                    final Frame f = entry.getValue().getFrame();
                    frames.add(new SyncFrame(f, s, fb));
                    if (f.isLocal()) {
                        if (frames_.get(f.getObjectId()) == null) {
                            frames_.put(f.getObjectId(), new ArrayList<>());
                        }
                        frames_.get(f.getObjectId()).add(f.getFrameData());
                    }
                } catch (MissingSyncFrameException e) {
                    logger.debug("Unable to sync frame " + ((FrameData) entry.getValue()).getPtr() + " because removed by freeing");
                }
                if (fb != null) {
                    fframes.add(fb);
                }
            }

            for (Map.Entry<Integer, List<FrameApi>> entry : frames_.entrySet()) {
                SQLCursor.addStreamFrame(new ContainerFrame(entry.getKey(), entry.getValue()));
            }

            SyncTask[] tasklist = new SyncTask[famt];

            int cnt = 0;
            for (Map.Entry e : Storage.getStorage().getFiles().entrySet()) {
                tasklist[cnt] = new SyncTask((DataFile) e.getValue());
                cnt++;
            }

            for (SyncTask task : tasklist) {
                for (SyncFrame bd : frames) {
                    if (task.getDataFile().getFileId() == (int) bd.getFile()) {
                        task.add(bd);
                    }
                }
            }

            long t1 = System.currentTimeMillis();
            pool.invokeAll(Arrays.asList(tasklist));
            long t2 = System.currentTimeMillis();
            logger.info("sync procedure was completed in " + (t2 - t1) + "ms");
            llt.commit();
            Storage.getStorage().clearJournal();
            for (FreeFrame fb : fframes) {
                s.persist(fb);
            }

            //todo async process must depends from stop() method

            final List<SyncFrame> dframes = frames.stream().filter(p -> p.isDistributed()).collect(Collectors.toList());
            pool2.submit(new TransportSyncTask(dframes));

            running = false;
            return true;
        }
    }

    public void commit() throws Exception {
        while (!syncFramesFromQueue()) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void run () {
        Thread.currentThread().setName("interference-sync-thread-"+Thread.currentThread().getId());
        while (f) {
            latch = new CountDownLatch(1);
            try {
                syncFramesFromQueue();
            } catch(Exception e) {
                e.printStackTrace();
            }

            try {
                final int period = Config.getConfig().SYNC_PERIOD;
                Thread.sleep(period);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            latch.countDown();
        }
    }

    public void stop() throws InterruptedException{
        f = false;
        if (latch != null) {
            latch.await();
        }
    }

}
