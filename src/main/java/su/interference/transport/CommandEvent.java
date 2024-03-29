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
import su.interference.core.Instance;
import su.interference.persistent.FrameData;
import su.interference.persistent.Session;
import su.interference.persistent.Table;
import su.interference.persistent.Transaction;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class CommandEvent extends TransportEventImpl implements PersistentEvent {

    private final static long serialVersionUID = 4363987969365366565L;
    private final static Logger logger = LoggerFactory.getLogger(CommandEvent.class);
    public static final int INITTRAN = 1;
    public static final int COMMIT = 2;
    public static final int ROLLBACK = 3;
    public static final int LOCK_TABLE = 4;
    public static final int UNLOCK_TABLE = 5;
    public static final int LOCK_FRAME = 6;
    public static final int UNLOCK_FRAME = 7;
    public static final int MAX_COMMAND = 10;
    private final int command;
    private final int nodeId;
    private final long id;
    private final long id2;

    public CommandEvent(int command, int nodeId, long id, int channelId) {
        super(channelId);
        this.command = command;
        this.nodeId = nodeId;
        this.id = id;
        this.id2 = 0;
    }

    public CommandEvent(int command, int nodeId, long id, long id2, int channelId) {
        super(channelId);
        this.command = command;
        this.nodeId = nodeId;
        this.id = id;
        this.id2 = id2;
    }

    @Override
    public EventResult process() {
        final Session s = Session.getSession();
        if (command == INITTRAN) {
            updateTransaction(this.id, s);
        }
        if (command == COMMIT) {
            final Transaction t = Instance.getInstance().getTransactionById(id);
            if (t != null) {
                t.commit(s, true);
            } else {
                return new EventResult(TransportCallback.FAILURE, null, 0, null, new RuntimeException(TransportContext.TRANSACTION_ISNULL_MESSAGE), null);
            }
        }
        if (command == ROLLBACK) {
            final Transaction t = Instance.getInstance().getTransactionById(id);
            if (t != null) {
                t.rollback(s, true);
            } else {
                return new EventResult(TransportCallback.FAILURE, null, 0, null, new RuntimeException(TransportContext.TRANSACTION_ISNULL_MESSAGE), null);
            }
        }
        if (command == LOCK_TABLE) {
            final Table t = Instance.getInstance().getTableById((int)id2);
            if (!t.lock(id)) {
                return new EventResult(TransportCallback.FAILURE, null, 0, null, new RuntimeException(TransportContext.UNABLE_LOCK_TABLE_MESSAGE), null);
            }
        }
        if (command == UNLOCK_TABLE) {
            final Table t = Instance.getInstance().getTableById((int)id2);
            if (!t.unlock(id)) {
                return new EventResult(TransportCallback.FAILURE, null, 0, null, new RuntimeException(TransportContext.UNABLE_UNLOCK_TABLE_MESSAGE), null);
            }
        }
        if (command == LOCK_FRAME) {
            final FrameData bd = Instance.getInstance().getFrameById(id2);
            if (!bd.rlock(id, this.nodeId)) {
                return new EventResult(TransportCallback.FAILURE, null, 0, null, new RuntimeException(TransportContext.UNABLE_LOCK_FRAME_MESSAGE), null);
            }
        }
        if (command == UNLOCK_FRAME) {
            //final FrameData bd = Instance.getInstance().getFrameById(id);
            //bd.unlock(this.nodeId);
        }
        return new EventResult(TransportCallback.SUCCESS, null, 0, null, null, null);
    }

    public long getId() {
        return id;
    }

    private void updateTransaction(long rtran, Session s) {
        final Transaction tran = Instance.getInstance().getTransactionById(rtran);
        if (tran == null) {
            final Transaction transaction = new Transaction();
            transaction.setSid(s.getSid());
            transaction.setTransId(rtran);
            try {
                s.persist(transaction);
            } catch (Exception e) {
                logger.error("unable to persist remote transaction", e);
            }
        }
    }

}
