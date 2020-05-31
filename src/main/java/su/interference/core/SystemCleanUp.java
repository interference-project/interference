package su.interference.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.metrics.Metrics;
import su.interference.persistent.FrameData;
import su.interference.persistent.Table;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class SystemCleanUp implements Runnable, ManagedProcess {
    private volatile boolean f = true;
    CountDownLatch latch;
    private final static Logger logger = LoggerFactory.getLogger(SystemCleanUp.class);
    private static final int CLEANUP_TIMEOUT = 3000;
    public static final int DATA_RETRIEVED_PRIORITY = 600;
    public static final int INDEX_RETRIEVED_PRIORITY = 900;
    private static final int CLEANUP_PROTECTION_THR = 1000;

    public void run () {
        while (f) {
            latch = new CountDownLatch(1);
            try {
                cleanUpFrames();
            } catch(Exception e) {
                e.printStackTrace();
            }

            try {
                //final int period = Config.getConfig().SYNC_PERIOD;
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
        for (Object entry : Instance.getInstance().getFramesMap().entrySet()) {
            final FrameData f = (FrameData) ((DataChunk) ((Map.Entry) entry).getValue()).getEntity();
            final long frameAmount = f.getDataObject().getFrameAmount();
            if (f.getDataFile().isData() || f.getDataFile().isIndex()) {
                f.decreasePriority();
                if (f.isSynced() && f.getObjectId() > 999 && f.getPriority() == 0 && frameAmount > CLEANUP_PROTECTION_THR) {
                    if (f.clearFrame()) {
                        i++;
                    }
                }
            }
            if (f.getDataFile().isTemp()) {
                if (f.isSynced() && f.getObjectId() > 999 && frameAmount > CLEANUP_PROTECTION_THR) {
                    if (f.clearFrame()) {
                        i++;
                    }
                }
            }
            if (f.getDataFile().isUndo()) {
                if (f.isSynced() && f.getObjectId() > 999) {
                    if (f.clearFrame()) {
                        i++;
                    }
                }
            }
        }
        Metrics.get("сleanUpBlocks").put(i);
        Metrics.get("systemCleanUp").stop();

    }
}
