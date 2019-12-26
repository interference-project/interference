/**
 The MIT License (MIT)

 Copyright (c) 2010-2019 head systems, ltd

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
import su.interference.core.Frame;
import su.interference.core.Instance;
import su.interference.persistent.Cursor;
import su.interference.persistent.Session;
import su.interference.sql.*;
import su.interference.sqlexception.SQLException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SQLEvent extends TransportEventImpl {
    private final static Logger logger = LoggerFactory.getLogger(SQLEvent.class);
    private final long cursorId;
    private final int targetId;
    private final Map<String, FrameApiJoin> joins;
    private final String rightType;
    private final String sql;
    private final String resultTargetName;
    private final long tranId;
    private final boolean execute;

    public SQLEvent(int channelId, long cursorId, Map<String, FrameApiJoin> joins, String rightType, int targetId, String sql, String resultTargetName, long tranId, boolean execute) {
        super(channelId);
        this.cursorId = cursorId;
        this.joins = joins;
        this.rightType = rightType;
        this.targetId = targetId;
        this.sql = sql;
        this.resultTargetName = resultTargetName;
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
            final FrameIterator rbi = d == null ? null : d.getRbi();
            final List<FrameApiJoin> res = new ArrayList<>();
            FrameApi b_ = null;
            for (Map.Entry<String, FrameApiJoin> entry : joins.entrySet()) {
                final FrameApiJoin j = entry.getValue();
                if (j.getRightAllocId() == 0 && rbi != null && (rightType.equals("SQLHashMapFrame") || rightType.equals("SQLIndexFrame"))) {
                    rbi.resetIterator();
                    try {
                        b_ = rbi.nextFrame();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                final FrameApi bd1 = Instance.getInstance().getFrameByAllocId(j.getLeftAllocId());
                final FrameApi bd2 = j.getRightAllocId() == 0 ? b_ : Instance.getInstance().getFrameByAllocId(j.getRightAllocId());
                try {
                    j.setResult(c.execute(bd1, bd2));
                    res.add(j);
                } catch (Exception e) {
                    return new EventResult(TransportCallback.FAILURE, this.cursorId, null, e);
                }
            }
            return new EventResult(TransportCallback.SUCCESS, this.cursorId, res, null);
        } else {
            final Session s = Session.getSession();
            final Cursor cur = this.cursorId == 0 ? null : new Cursor(this.sql, this.resultTargetName, Cursor.SLAVE_TYPE);
            if (cur != null) {
                cur.setCursorId(cursorId);

                try {
                    s.persist(cur);

                    cur.setSession(s);
                    cur.setTargetNodeId(this.channelId);
                    cur.setTargetId(this.targetId);

                    cur.startStatement(this.tranId);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
            //s.startStatement();
            final SQLStatement sql = cur == null ? SQLStatementFactory.getInstance(this.sql, s) : SQLStatementFactory.getInstance(this.sql, cur, s);
            //todo temp solution
            if (sql.getSQLException() != null) {
                return new EventResult(TransportCallback.FAILURE, cur.getCursorId(), null, sql.getSQLException());
            } else {
                try {
                    if (sql instanceof SQLSystem) {
                        sql.executeSQL(s);
                    }
                } catch (SQLException e) {
                    return new EventResult(TransportCallback.FAILURE, cur.getCursorId(), null, e);
                }
                return new EventResult(TransportCallback.SUCCESS, cur.getCursorId(), null, null);
            }
        }
    }

}
