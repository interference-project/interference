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
import su.interference.core.EntityContainer;
import su.interference.core.Instance;
import su.interference.exception.InternalException;
import su.interference.persistent.Session;
import su.interference.core.GenericResultImpl;
import su.interference.proxy.ClassContainer;
import su.interference.sql.ResultSet;

import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class RemoteEvent extends TransportEventImpl {
    private final static long serialVersionUID = 903562217038267154L;
    private final static Logger logger = LoggerFactory.getLogger(RemoteEvent.class);
    private final static Field[] fields = RemoteEvent.class.getDeclaredFields();
    public static final int REMOTE_SESSION = 1;
    public static final int REMOTE_PERSIST = 2;
    public static final int REMOTE_FIND = 3;
    public static final int REMOTE_EXECUTE = 4;
    public static final int REMOTE_RSPOLL = 5;
    public static final int REMOTE_COMMIT = 6;
    public static final int REMOTE_ROLLBACK = 7;
    public static final int REMOTE_REGISTER = 8;

    private final Object source;
    private final String srcClass;
    private int type;
    private String callbackHost;

    public RemoteEvent(Object source, String srcClass, int type, String callbackHost) {
        super(0);
        this.source = source;
        this.srcClass = srcClass;
        this.type = type;
        this.callbackHost = callbackHost;
    }

    public RemoteEvent() {
        super(0);
        this.source = null;
        this.srcClass = null;
        this.type = 0;
        this.callbackHost = null;
    }

    @Override
    public EventResult process() {
        if (this.type == REMOTE_SESSION) {
            final TransportChannel channel = new TransportChannel(this.callbackHost);
            Session s = null;
            try {
                final CountDownLatch latch = new CountDownLatch(1);
                channel.start(latch);
                latch.countDown();
                s = Session.getSession(channel);
            } catch (Exception e) {
                logger.error("RemoteEvent process", e);
                return new EventResult(TransportCallback.FAILURE, null, 0, null, e, channel);
            }
            return new EventResult(TransportCallback.SUCCESS, s.getSessionId(), 0, null, null, channel);
        } else {
            final Session s = Instance.getInstance().getSession(this.callbackHost);
            Object resultObject = null;
            try {
                if (this.type == REMOTE_PERSIST) {
                    final DataChunk dc = new DataChunk((byte[]) this.source, Instance.getInstance().getTableByName(this.srcClass), false);
                    final Object o = dc.getEntity();
                    ((EntityContainer) o).setDataChunk(null);
                    if (o != null) {
                        s.persist(o);
                    }
                } else
                if (this.type == REMOTE_FIND) {
                    final DataChunk dc = ((EntityContainer) s.find(this.srcClass, (long) this.source)).getDataChunk();
                    resultObject = dc.getChunk();
                } else
                if (this.type == REMOTE_EXECUTE) {
                    final ResultSet rs = s.execute((String) this.source);
                    resultObject = UUID.randomUUID().toString();
                    s.putResultSet((String) resultObject, rs);
                } else
                if (this.type == REMOTE_RSPOLL) {
                    final ResultSet rs = s.getResultSet((String) this.source);
                    final GenericResultImpl gr = (GenericResultImpl) rs.poll(s);
                    resultObject = gr == null ? null : gr.getGenericObject();
                } else {
                if (this.type == REMOTE_COMMIT) {
                    s.commit();
                } else
                if (this.type == REMOTE_ROLLBACK) {
                    s.rollback();
                } else
                if (this.type == REMOTE_REGISTER) {
                    final ClassContainer cc = (ClassContainer) this.source;
                    s.registerTable(cc.getName(), cc, s, null, null, null, false);
                } else
                    return new EventResult(TransportCallback.FAILURE, null, 0, null, new InternalException(), s.getSessionChannel());
                }
            } catch (Exception e) {
                logger.error("RemoteEvent process", e);
                return new EventResult(TransportCallback.FAILURE, null, 0, null, e, s.getSessionChannel());
            }
            return new EventResult(TransportCallback.SUCCESS, resultObject, 0, null, null, s.getSessionChannel());
        }
    }

    public Object getSource() {
        return source;
    }

    public String getSrcClass() {
        return srcClass;
    }

    public int getType() {
        return type;
    }

    public String getCallbackHost() {
        return callbackHost;
    }
}
