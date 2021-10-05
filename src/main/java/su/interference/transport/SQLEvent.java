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
import su.interference.persistent.Cursor;
import su.interference.persistent.Session;
import su.interference.sql.*;
import su.interference.sqlexception.SQLException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SQLEvent extends TransportEventImpl {
    private final static long serialVersionUID = 436398796004891261L;
    private final static Logger logger = LoggerFactory.getLogger(SQLEvent.class);
    private final long cursorId;
    private final int targetId;
    private final Map<String, FrameApiJoin> joins;
    private final String rightType;
    private final String sql;
    private final List<String> resultTargetNames;
    private final long tranId;
    private final boolean execute;

    public SQLEvent(int channelId, long cursorId, Map<String, FrameApiJoin> joins, String rightType, int targetId, String sql, List<String> resultTargetNames, long tranId, boolean execute) {
        super(channelId);
        this.cursorId = cursorId;
        this.joins = joins;
        this.rightType = rightType;
        this.targetId = targetId;
        this.sql = sql;
        this.resultTargetNames = resultTargetNames;
        this.tranId = tranId;
        this.execute = execute;
    }

    @Override
    public EventResult process() {
        if (this.execute) {
            final Cursor cur = Instance.getInstance().getCursorById(cursorId);
            final SQLSelect sql = cur.getSqlStmt();
            //now, only first cursor mau contains left FS table
            final SQLCursor c = sql.getSQLCursorById(1);
            final SQLJoinDispatcher d = c.getHmap();
            final List<FrameApiJoin> res = new ArrayList<>();
            final List<Future> flist = new ArrayList<>();
            try {
                for (Map.Entry<String, FrameApiJoin> entry : joins.entrySet()) {
                    final FrameApiJoin j = entry.getValue();
                    FrameApi b = null;
                    FrameApi b_ = null;
                    if (c.getLbi() instanceof SQLIndex) {
                        b = ((SQLIndex) c.getLbi()).getFrameByAllocId(j.getLeftAllocId());
                    }
                    if (j.getRightAllocId() == 0 && c.getRbi() != null && (rightType.equals("SQLHashMapFrame") || rightType.equals("SQLIndexFrame"))) {
                        c.getRbi().resetIterator();
                        b_ = c.getRbi().nextFrame();
                    }
                    final FrameApi bd1 = c.getLbi() instanceof SQLIndex ? b : Instance.getInstance().getFrameByAllocId(j.getLeftAllocId());
                    final FrameApi bd2 = j.getRightAllocId() == 0 ? b_ : Instance.getInstance().getFrameByAllocId(j.getRightAllocId());

                    // todo may caused by ghost (non-freed) allocIds resulting on some nodes after distributed rollback
                    // todo this problem is known and should be fixed in future
                    if (bd1 == null) {
                        logger.error("SQLEvent unable to retrieve frame by allocId: " + j.getLeftAllocId());
                        return new EventResult(TransportCallback.FAILURE, null, this.cursorId, res, null, null);
                    }

                    flist.add(c.execute(c.getPbi(), bd1, bd2, j));
                    res.add(j);
                }
                boolean cnue = true;
                while (cnue) {
                    cnue = false;
                    for (Future f : flist) {
                        if (!f.isDone()) {
                            cnue = true;
                        }
                    }
                    Thread.sleep(10);
                }
            } catch (Exception e) {
                logger.error("SQLEvent process: ", e);
                return new EventResult(TransportCallback.FAILURE, null, this.cursorId, res, null, null);
            }
            return new EventResult(TransportCallback.SUCCESS, null, this.cursorId, res, null, null);
        } else {
            final Session s = Session.getSession();
            final Cursor cur = this.cursorId == 0 ? null : new Cursor(this.sql, this.resultTargetNames, Cursor.SLAVE_TYPE);
            if (cur != null) {
                cur.setCursorId(cursorId);

                try {
                    s.persist(cur);

                    cur.setSession(s);
                    cur.setTargetNodeId(this.channelId);
                    cur.setTargetId(this.targetId);

                    cur.startStatement(this.tranId);
                } catch (Exception e) {
                    logger.error("SQLEvent process: ", e);
                }

            }
            //s.startStatement();
            final SQLStatement sql = cur == null ? SQLStatementFactory.getInstance(this.sql, s) : SQLStatementFactory.getInstance(this.sql, cur, s);
            //todo temp solution
            if (sql.getSQLException() != null) {
                return new EventResult(TransportCallback.FAILURE, null, cur.getCursorId(), null, sql.getSQLException(), null);
            } else {
                try {
                    if (sql instanceof SQLSystem) {
                        sql.executeSQL(s);
                    }
                } catch (SQLException e) {
                    return new EventResult(TransportCallback.FAILURE, null, cur.getCursorId(), null, e, null);
                }
                return new EventResult(TransportCallback.SUCCESS, null, cur.getCursorId(), null, null, null);
            }
        }
    }

}
