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

package su.interference.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.persistent.Table;
import su.interference.persistent.FrameData;
import su.interference.core.Instance;
import su.interference.sqlexception.InvalidTableDescription;
import su.interference.sqlexception.MissingTableInSerializableMode;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.net.MalformedURLException;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SQLTable implements Comparable, FrameIterator {

    private final static Logger logger = LoggerFactory.getLogger(SQLTable.class);
    private final Table table;
    private SQLSelect sql; // inline view
    private final String alias;
    private volatile LinkedBlockingQueue<FrameData> frames;
    private final AtomicBoolean terminate;
    private boolean leftfs;

    public SQLTable (String table, String alias) throws InvalidTableDescription, MissingTableInSerializableMode {
        this.alias = alias;
        String[] tblss = table.trim().split("\\.");
        if (tblss.length==1) { //without schema prefix - use user default schema
            this.table = Instance.getInstance().getTableByName(table);
        } else {
            this.table = Instance.getInstance().getTableByName(table);
        }
        if (this.table == null) {
            throw new InvalidTableDescription();
        }

        frames = Instance.getInstance().getTableById(this.table.getObjectId()).getFrames();
        this.terminate = new AtomicBoolean(false);
    }

    public int getType() {
        return FrameIterator.TYPE_TABLE;
    }

    public boolean isIndex() throws MalformedURLException, ClassNotFoundException { return table.isIndex(); }

    public int getObjectId() {
        return table.getObjectId();
    }

    public List<Integer> getObjectIds() {
        final List<Integer> objectIds = new ArrayList<Integer>();
        objectIds.add(table.getObjectId());
        return objectIds;
    }

    //DESC sorting on FrameAmount
    public int compareTo(Object obj) {
        SQLTable t = (SQLTable)obj;
        if (this.getTable().getFrameAmount() > t.getTable().getFrameAmount()) {
            return -1;
        } else if (this.getTable().getFrameAmount() < t.getTable().getFrameAmount()) {
            return 1;
        }
        return 0;
    }

    public synchronized FrameData nextFrame() throws Exception {
        if (hasNextFrame()) {
            final FrameData bd = frames.take();
            if (bd.getObjectId() == 0 && bd.getFrameId() == 0) {
                terminate.compareAndSet(false, true);
                return null;
            }
            logger.debug(this.table.getName()+" returns next frame with allocId = "+bd.getAllocId());
            return bd;
        }
        return null;
    }

    public synchronized void resetIterator() {
        if (!hasNextFrame()) {
            try {
                frames = Instance.getInstance().getTableById(this.table.getObjectId()).getFrames();
                terminate.compareAndSet(true, false);
            } catch (Exception e) {
                throw new RuntimeException();
            }
        }
    }

    public synchronized boolean hasNextFrame() {
        if (terminate.get()) {
            return false;
        }
        return true;
    }

    public Table getTable() {
        return table;
    }

    public SQLSelect getSql() {
        return sql;
    }

    public void setSql(SQLSelect sql) {
        this.sql = sql;
    }

    public String getAlias() {
        return alias;
    }

    @Override
    public boolean isLeftfs() {
        return leftfs;
    }

    @Override
    public void setLeftfs(boolean leftfs) {
        this.leftfs = leftfs;
    }
}
