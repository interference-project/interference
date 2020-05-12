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
                Thread.sleep(3000);
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
            f.decreasePriority();
            if (f.isSynced() && f.getObjectId() > 999 && f.getPriority() <= 0) {
                f.clearFrame();
            }
            i++;
        }
        Metrics.get("сleanUpBlocks").put(i);
        Metrics.get("systemCleanUp").stop();
    }
}
