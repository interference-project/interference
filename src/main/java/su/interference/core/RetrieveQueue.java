package su.interference.core;

import java.util.concurrent.LinkedBlockingQueue;

public class RetrieveQueue {
    private final LinkedBlockingQueue<Chunk> q;
    private final ManagedCallable r;
    private boolean retrieve = true;

    public RetrieveQueue(LinkedBlockingQueue<Chunk> q, ManagedCallable r) {
        this.q = q;
        this.r = r;
    }

    public Object poll() {
        try {
            if (retrieve) {
                Chunk c = q.take();
                if (c.isTerminate()) {
                    retrieve = false;
                    return null;
                }
                return c.getEntity();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public Chunk cpoll() {
        try {
            if (retrieve) {
                Chunk c = q.take();
                if (c.isTerminate()) {
                    retrieve = false;
                    return null;
                }
                return c;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void stop() {
        retrieve = false;
        if (r != null) {
            r.stop();
        }
    }

    public LinkedBlockingQueue<Chunk> getQ() {
        return q;
    }

    public ManagedCallable getR() {
        return r;
    }

    public boolean isRetrieve() {
        return retrieve;
    }
}
