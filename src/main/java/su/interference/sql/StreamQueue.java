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

import su.interference.core.Chunk;
import su.interference.core.DataChunk;
import su.interference.persistent.Session;
import su.interference.persistent.Table;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class StreamQueue implements ResultSet {
    private final List<SQLColumn> rscols;
    private final Table rstable;
    @SuppressWarnings("unchecked")
    private final LinkedBlockingQueue<Object> q = new LinkedBlockingQueue<>(10000);
    private final Session s;
    private SQLColumn windowColumn;
    private int windowInterval;
    private boolean running;

    public StreamQueue(List<SQLColumn> rscols, Table rstable, SQLColumn windowColumn, Session s) {
        this.rscols = rscols;
        this.rstable = rstable;
        this.s = s;
        this.windowColumn = windowColumn;
        this.windowInterval = windowColumn == null ? 0 : windowColumn.getWindowInterval();
        this.running = true;
    }

    public List<SQLColumn> getRscols() {
        return rscols;
    }

    public Table getRstable() {
        return rstable;
    }

    public SQLColumn getWindowColumn() {
        return windowColumn;
    }

    public int getWindowInterval() {
        return windowInterval;
    }

    public boolean isRunning() {
        return running;
    }

    public void stop() {
        this.running = false;
        s.closeStreamQueue();
        s.setStream(false);
    }

    public DataChunk persist(Object o, Session s) throws Exception {
        q.put(o);
        return null;
    }

    public Object poll(Session s) throws InterruptedException {
        if (!running) {
            return null;
        }
        final Object o = q.take();
        if (o instanceof ResultSetTerm) {
            running = false;
            return null;
        }
        return o;
    }

    public Chunk cpoll(Session s) {
        return null;
    }

    public int getObjectId() {
        return 0;
    }

    public boolean isIndex() {
        return false;
    }

    public Class getTableClass() {
        return null;
    }

    public java.lang.reflect.Field[] getFields() {
        return null;
    }

    public void deallocate(Session s) throws Exception {
        //todo
    }

    public boolean isPersistent() {
        return false;
    }

    public void clearPersistent() {
        //unused
    }

}
