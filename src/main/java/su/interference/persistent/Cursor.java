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

package su.interference.persistent;

import su.interference.core.*;
import su.interference.exception.InternalException;
import su.interference.mgmt.MgmtColumn;
import su.interference.sql.SQLSelect;

import javax.persistence.*;
import java.io.Serializable;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

@Entity
@SystemEntity
@DisableSync
public class Cursor implements Serializable {
    @Transient
    public static final int MASTER_TYPE     = 1;
    @Transient
    public static final int SLAVE_TYPE      = 2;
    @Transient
    public static final int STATE_IDLE      = 1;
    @Transient
    public static final int STATE_PREPARED  = 2;
    @Transient
    public static final int STATE_RUNNING   = 3;
    @Transient
    public static final int STATE_COMPLETED = 4;

    @Column
    @Id
    @IndexColumn
    @GeneratedValue
    @DistributedId
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private long cursorId;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private String sql;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int targetNodeId;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int targetId;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private String targetClassName;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int type;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int state;

    @Transient
    private SQLSelect sqlStmt;
    @Transient
    private String resultTargetName;
    @Transient
    private Session session;
    @Transient
    public static final int CLASS_ID = 14;

    public static int getCLASS_ID() {
        return CLASS_ID;
    }

    public Cursor() {
        this.state = STATE_IDLE;
    }

    public Cursor(String sql, String resultTargetName, int type) {
        this.state = STATE_IDLE;
        this.sql = sql;
        this.resultTargetName = resultTargetName;
        this.type = type;
    }

    public Cursor(String sql, int type) {
        this.state = STATE_IDLE;
        this.cursorId = cursorId;
        this.sql = sql;
        this.type = type;
    }

    //constructor for low-level storage function (initial first-time load table descriptions from datafile)
    public Cursor (DataChunk chunk) throws IllegalAccessException, ClassNotFoundException, InternalException, MalformedURLException {
        final Object[] dcs = chunk.getDcs().getValueSet();
        final Class c = this.getClass();
        final java.lang.reflect.Field[] f = c.getDeclaredFields();
        int x = 0;
        for (int i=0; i<f.length; i++) {
            final Transient ta = f[i].getAnnotation(Transient.class);
            if (ta==null) {
                final int m = f[i].getModifiers();
                if (Modifier.isPrivate(m)) {
                    f[i].setAccessible(true);
                }
                f[i].set(this, dcs[x]);
                x++;
            }
        }
    }

    public long getCursorId() {
        return cursorId;
    }

    public void setCursorId(long cursorId) {
        this.cursorId = cursorId;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getTargetClassName() {
        return targetClassName;
    }

    public void setTargetClassName(String targetClassName) {
        this.targetClassName = targetClassName;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public SQLSelect getSqlStmt() {
        return sqlStmt;
    }

    public void setSqlStmt(SQLSelect sqlStmt) {
        this.sqlStmt = sqlStmt;
    }

    public String getResultTargetName() {
        return resultTargetName;
    }

    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public int getTargetNodeId() {
        return targetNodeId;
    }

    public void setTargetNodeId(int targetNodeId) {
        this.targetNodeId = targetNodeId;
    }

    public int getTargetId() {
        return targetId;
    }

    public void setTargetId(int targetId) {
        this.targetId = targetId;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public void startStatement(long tranId) throws Exception {
        this.session.startStatement(tranId);
    }
}
