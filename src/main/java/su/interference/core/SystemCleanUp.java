package su.interference.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.metrics.Metrics;
import su.interference.persistent.FrameData;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class SystemCleanUp implements Runnable, ManagedProcess {
    private volatile boolean f = true;
    CountDownLatch latch;
    private final static Logger logger = LoggerFactory.getLogger(SystemCleanUp.class);
    private static final int CLEANUP_TIMEOUT = 3000;
    public static final int DATA_RETRIEVED_PRIORITY = 6;
    public static final int INDEX_RETRIEVED_PRIORITY = 9;
    private static final int CLEANUP_PROTECTION_THR = 100;

    public void run () {
        Thread.currentThread().setName("interference-cleanup-thread");
        while (f) {
            latch = new CountDownLatch(1);
            try {
                cleanUpFrames();
            } catch(Exception e) {
                e.printStackTrace();
            }

            try {
                Thread.sleep(CLEANUP_TIMEOUT);
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

    private void cleanUpFrames() {

        Metrics.get("systemCleanUp").start();
        int i = 0;
        int d = 0;
        int x = 0;
        int u = 0;
        int i_ = 0;
        int d_ = 0;
        int x_ = 0;
        int u_ = 0;
        for (Object entry : Instance.getInstance().getFramesMap().entrySet()) {
            final FrameData f = (FrameData) ((DataChunk) ((Map.Entry) entry).getValue()).getEntity();
            final long frameAmount = f.getDataObject().getFrameAmount();
            if (f.getDataFile().isData()) {
                f.decreasePriority();
                if (f.isSynced() && f.getObjectId() > 999 && f.getPriority() == 0 && frameAmount > CLEANUP_PROTECTION_THR) {
                    if (f.clearFrame()) {
                        d++;
                    }
                }
                if (f.isFrame()) {
                    d_++;
                }
            }
            if (f.getDataFile().isIndex()) {
                f.decreasePriority();
                if (f.isSynced() && f.getObjectId() > 999 && f.getPriority() == 0 && frameAmount > CLEANUP_PROTECTION_THR) {
                    if (f.clearFrame()) {
                        x++;
                    }
                }
                if (f.isFrame()) {
                    x_++;
                }
            }
            if (f.getDataFile().isTemp()) {
                if (f.isSynced() && f.getObjectId() > 999 && frameAmount > CLEANUP_PROTECTION_THR) {
                    if (f.clearFrame()) {
                        i++;
                    }
                }
                if (f.isFrame()) {
                    i_++;
                }
            }
            if (f.getDataFile().isUndo()) {
                if (f.isSynced() && frameAmount > CLEANUP_PROTECTION_THR) {
                    if (f.clearFrame()) {
                        u++;
                    }
                }
                if (f.isFrame()) {
                    u_++;
                }
            }
        }
        Metrics.get("сleanUpDataFrames").put(d);
        Metrics.get("сleanUpIndexFrames").put(x);
        Metrics.get("сleanUpUndoFrames").put(u);
        Metrics.get("imDataFrames").put(d_);
        Metrics.get("imIndexFrames").put(x_);
        Metrics.get("imUndoFrames").put(u_);
        Metrics.get("systemCleanUp").stop();

    }
}
