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
import su.interference.core.FrameAllocComparator;
import su.interference.persistent.Table;
import su.interference.persistent.FrameData;
import su.interference.core.Instance;
import su.interference.sqlexception.InvalidTableDescription;
import su.interference.sqlexception.MissingTableInSerializableMode;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
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
    private long frameStart;
    private final int amount;
    private final List<FrameData> frames;
    private int cntrStart;
    private AtomicInteger framecntr;
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
        Collections.sort(frames, new FrameAllocComparator());
        logger.info(this.table.getName()+" total frames: "+frames.size());
        this.amount = frames.size();
        this.frameStart = frames.get(0).getFrameId();
        this.cntrStart = 0;
        this.framecntr = new AtomicInteger(0);
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

    public synchronized FrameData nextFrame() {
        if (hasNextFrame()) {
            FrameData bd = frames.get(framecntr.get());
            logger.debug(this.table.getName()+" returns next "+framecntr+" frame with allocId = "+bd.getAllocId());
            framecntr.incrementAndGet();
            return bd;
        }
        return null;
    }

    public synchronized void resetIterator() {
        if (!hasNextFrame()) {
            framecntr.set(0);
        }
    }

    public synchronized boolean hasNextFrame() {
        if ((this.amount+this.cntrStart) > framecntr.get()) {
            return true;
        }
        return false;
    }

    public int getFramesAmount() {
        return this.frames.size();
    }

    //start frameid for node partition
    public long getStartFrameId(int node, int nodes) {
        int psize = this.frames.size()/nodes;
        return this.frames.get(node*psize).getFrameId();
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

    public long getFrameStart() {
        return frameStart;
    }

    public void setFrameStart(long frameStart) {
        this.frameStart = frameStart;
        for (int i=0; i<this.frames.size(); i++) {
            if (this.frames.get(i).getFrameId()==frameStart) {
                this.cntrStart = i;
                this.framecntr = new AtomicInteger(i);
            }
        }
    }

    public int getAmount() {
        return amount;
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
