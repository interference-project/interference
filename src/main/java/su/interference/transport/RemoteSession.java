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

package su.interference.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.core.DataChunk;
import su.interference.core.GenericObject;
import su.interference.persistent.Table;
import su.interference.proxy.SimplePOJOProxyFactory;

import java.util.Map;
import java.util.concurrent.*;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class RemoteSession {
    private final static Logger logger = LoggerFactory.getLogger(RemoteSession.class);
    private final String sessionId;
    private final TransportChannel channel;
    private final TransportContext context;
    private final LinkedBlockingQueue<RemoteRequest> mq = new LinkedBlockingQueue<>();
    private final ExecutorService pool = Executors.newFixedThreadPool(1);
    private final Map<String, Table> tmap = new ConcurrentHashMap<>();

    public RemoteSession(String host, int port, String callbackHost, int callbackPort) throws InterruptedException {
        channel = new TransportChannel("0:"+host+":"+port);
        CountDownLatch latch = new CountDownLatch(1);
        channel.start(latch);
        latch.await();
        context = TransportContext.getInstance(channel, callbackPort);
        context.start();
        final RemoteEvent event = new RemoteEvent(null, null, RemoteEvent.REMOTE_SESSION, "0:"+callbackHost+":"+callbackPort);
        context.send(event);
        event.getLatch().await();
        if (event.getCallback() != null && event.getCallback().getResult().getResult() == TransportCallback.SUCCESS) {
            sessionId = (String) event.getCallback().getResult().getResultObject();
        } else {
            sessionId = null;
            logger.error("RemoteSession init", event.getProcessException());
        }
    }

    public Object find(Class c, long id) throws InterruptedException, ClassNotFoundException, NoSuchMethodException {
        final RemoteEvent event = new RemoteEvent(id, c.getName(), RemoteEvent.REMOTE_FIND, sessionId);
        context.send(event);
        event.getLatch().await();
        if (event.getProcessException() != null) {
            throw new RuntimeException(event.getProcessException());
        }
        final byte[] b = (byte[]) event.getCallback().getResult().getResultObject();
        final DataChunk dc = new DataChunk(b, getTableByName(c.getName(), c), true);
        return dc.getClientStandaloneEntity();
    }

    public RemoteResultSet execute(String sql) throws InterruptedException {
        final RemoteEvent event = new RemoteEvent(sql, null, RemoteEvent.REMOTE_EXECUTE, sessionId);
        context.send(event);
        event.getLatch().await();
        if (event.getProcessException() != null) {
            throw new RuntimeException(event.getProcessException());
        }
        return new RemoteResultSet((String) event.getCallback().getResult().getResultObject(), this);
    }

    public void persist(Object o) throws InterruptedException, ClassNotFoundException, NoSuchMethodException {
        final DataChunk dc = new DataChunk(o, null, getTableByName(o.getClass().getName(), o.getClass()), true);
        final RemoteEvent event = new RemoteEvent(dc.getChunk(), o.getClass().getName(), RemoteEvent.REMOTE_PERSIST, sessionId);
        context.send(event);
        event.getLatch().await();
        if (event.getProcessException() != null) {
            throw new RuntimeException(event.getProcessException());
        }
        return;
    }

    public void commit() throws InterruptedException {
        final RemoteEvent event = new RemoteEvent(null, null, RemoteEvent.REMOTE_COMMIT, sessionId);
        context.send(event);
        event.getLatch().await();
        if (event.getProcessException() != null) {
            throw new RuntimeException(event.getProcessException());
        }
    }

    public void rollback() throws InterruptedException {
        final RemoteEvent event = new RemoteEvent(null, null, RemoteEvent.REMOTE_ROLLBACK, sessionId);
        context.send(event);
        event.getLatch().await();
        if (event.getProcessException() != null) {
            throw new RuntimeException(event.getProcessException());
        }
    }

    public void register(Class c) throws InterruptedException, ClassNotFoundException {
        final SimplePOJOProxyFactory ppf = SimplePOJOProxyFactory.getInstance();
        final RemoteEvent event = new RemoteEvent(ppf.build(c.getName()), null, RemoteEvent.REMOTE_REGISTER, sessionId);
        context.send(event);
        event.getLatch().await();
        if (event.getProcessException() != null) {
            throw new RuntimeException(event.getProcessException());
        }
    }

    protected GenericObject rspoll(String uuid) throws InterruptedException {
        final RemoteEvent event = new RemoteEvent(uuid, null, RemoteEvent.REMOTE_RSPOLL, sessionId);
        context.send(event);
        event.getLatch().await();
        if (event.getProcessException() != null) {
            throw new RuntimeException(event.getProcessException());
        }
        final GenericObject gobj = (GenericObject) event.getCallback().getResult().getResultObject();
        return gobj;
    }

    private Table getTableByName(String name, Class c) throws ClassNotFoundException, NoSuchMethodException {
        final Table t = tmap.get(name);
        if (t == null) {
            final Table t_ = new Table(name);
            //todo concept violation, but client uses simple entity class now
            t_.setSc(c);
            tmap.put(name, t_);
            return t_;
        }
        return t;
    }

    @Override
    public String toString() {
        return this.sessionId;
    }
}
