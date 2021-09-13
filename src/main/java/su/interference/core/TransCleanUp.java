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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.persistent.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class TransCleanUp implements Runnable, ManagedProcess {
    private Session session = Session.getSession();
    private volatile boolean f = true;
    CountDownLatch latch;
    private final static Logger logger = LoggerFactory.getLogger(TransCleanUp.class);

    public void run () {
        Thread.currentThread().setName("interference-transactions-cleanup-thread-"+Thread.currentThread().getId());
        while (f) {
            latch = new CountDownLatch(1);
            try {
                cleanUp();
            } catch(Exception e) {
                e.printStackTrace();
            }

            try {
                Thread.sleep(Config.getConfig().TRANS_CLEANUP_TIMEOUT);
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

    private void cleanUp() {
        try {
            for (Transaction t : Instance.getInstance().getTransactions()) {
                ArrayList<Long> fptr = new ArrayList<>();
                if (t.getTransType() == Transaction.TRAN_THR && t.getCid() > 0) {
                    int i = 0;
                    for (TransFrame tf : Instance.getInstance().getTransFramesByTransId(t.getTransId())) {
                        if (t.isLocal()) {
                            boolean hasfb = false;
                            if (tf.getUframeId() > 0) {
                                for (Long f : fptr) {
                                    if (f == tf.getUframeId()) {
                                        hasfb = true;
                                        break;
                                    }
                                }
                                final List<RetrieveLock> rls = Instance.getInstance().getRetrieveLocksByObjectId(tf.getObjectId());
                                if (rls.size() == 0) {
                                    //deallocate undo frame
                                    final FrameData ub = Instance.getInstance().getFrameById(tf.getUframeId());
                                    //store frame params as free
                                    //todo undo space may not be not deallocated - check ub for not null
                                    if (!hasfb && ub != null) {
                                        final FreeFrame fb = new FreeFrame(0, tf.getUframeId(), ub.getSize());
                                        session.persist(fb); //insert
                                        session.delete(ub);
                                        fptr.add(tf.getUframeId());
                                    }
                                }
                            }
                        }
                        session.delete(tf);
                        i++;
                    }
                    t.setTransType(Transaction.TRAN_LEGACY);
                    session.persist(t);
                    logger.info(i+" transaction frames removed for transaction id = "+t.getTransId());
                }
            }
        } catch (Exception e) {
            logger.error("transaction cleanup process", e);
        }
    }

}
