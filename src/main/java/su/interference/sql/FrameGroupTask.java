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
import su.interference.core.DataChunk;
import su.interference.persistent.Cursor;
import su.interference.persistent.Session;
import su.interference.persistent.Table;
import su.interference.core.GenericResultImpl;

import java.util.Queue;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class FrameGroupTask implements Runnable {

    private final static Logger logger = LoggerFactory.getLogger(FrameGroupTask.class);
    private final Cursor cur;
    private final Queue<Object> q;
    private final ResultSet target;
    private final Table gtable;
    private final Session s;

    public FrameGroupTask(Cursor cur, Queue<Object> q, ResultSet target, Table gtable, Session s) {
        this.cur = cur;
        this.q = q;
        this.target = target;
        this.gtable = gtable;
        this.s = s;
    }

    @Override
    public void run() {

        final boolean ixflag = cur.getSqlStmt().getCols().getOrderColumns().size() > 0;
        Thread.currentThread().setName("interference-sql-group-thread-"+Thread.currentThread().getId());

        try {
            final SQLGroupContainer sqlg = new SQLGroupContainer(((StreamQueue) target).getRscols(),
                    cur.getSqlStmt().getCols().getGroupColumns());

            while (((StreamQueue) target).isRunning()) {
                GenericResultImpl o = (GenericResultImpl) q.poll();

                if (o != null) {
                    final DataChunk gdc = sqlg.add(o.getDataChunk(s), gtable, s);
                    if (gdc != null) {
                        Object oo = gdc.getEntity();
                        target.persist(oo, s);
                    }

                    if (q.peek() == null) {
                        Thread.sleep(100);
                    }
                }
            }
            //return last group after stop
/*
            final DataChunk gdc = sqlg.add(null);
            if (gdc != null) {
                Object oo = gdc.getEntity(gtable);
                target.persist(oo, s);
            }
*/


        } catch (Exception e) {
            ((StreamQueue) target).stop();
            logger.error("", e);
        }
    }

}
