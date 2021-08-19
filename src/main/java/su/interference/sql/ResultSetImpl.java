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

package su.interference.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.core.Chunk;
import su.interference.core.Config;
import su.interference.core.DataChunk;
import su.interference.exception.InternalException;
import su.interference.persistent.Session;
import su.interference.persistent.Table;

import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class ResultSetImpl implements ResultSet {
    private final static Logger logger = LoggerFactory.getLogger(ResultSetImpl.class);
    private final Table target;
    private final boolean persistent;
    private final LinkedBlockingQueue q = new LinkedBlockingQueue(Config.getConfig().RETRIEVE_QUEUE_SIZE);
    private boolean started = true;
    private CountDownLatch latch;
    private final SQLCursor sqlc;
    private Queue<Chunk> q_;

    public ResultSetImpl(Table target, SQLCursor sqlc, boolean persistent) {
        this.target = target;
        this.persistent = persistent;
        this.sqlc = sqlc;
    }

    @SuppressWarnings("unchecked")
    public DataChunk persist(Object o, Session s) throws Exception {
        if (persistent) {
            return target.persist(o, s);
        } else {
            final boolean success = q.offer(o);
            if (!success) {
                q.put(o);
            }
        }
        return null;
    }

    public Object poll(Session s) throws Exception {
        if (persistent) {
            if (sqlc != null && latch == null) {
                latch = new CountDownLatch(1);
                sqlc.flushTarget();
            }
            if (latch != null) {
                latch.await();
            }
            return target.poll(s);
        } else {
            if (sqlc != null && !sqlc.isFlush()) {
                sqlc.flushTarget();
            }
            if (started) {
                final Object o = q.take();
                if (o instanceof ResultSetTerm) {
                    started = false;
                    return null;
                }
                return o;
            } else {
                return null;
            }
        }
    }

    public Chunk cpoll(Session s) throws Exception {
        if (persistent) {
            if (sqlc != null && latch == null) {
                latch = new CountDownLatch(1);
                sqlc.flushTarget();
            }
            if (latch != null) {
                latch.await();
            }
            return target.cpoll(s);
        } else {
            return null;
        }
    }

    public Table getTarget() {
        return target;
    }

    public boolean isPersistent() {
        return persistent;
    }

    public void release() {
        if (latch != null) {
            latch.countDown();
        }
    }

    public int getObjectId() {
        return target.getObjectId();
    }

    public boolean isIndex() {
        return target.isIndex();
    }

    public Class getTableClass() {
        return target.getTableClass();
    }

    public java.lang.reflect.Field[] getFields() throws InternalException {
        return target.getFields();
    }

    public void deallocate(Session s) throws Exception {
        this.target.deallocate(s);
    }
}
