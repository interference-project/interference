package su.interference.sql;

import su.interference.core.DataChunk;
import su.interference.core.EntityContainer;
import su.interference.persistent.Cursor;
import su.interference.persistent.Session;
import su.interference.persistent.Table;

import java.util.Queue;

public class FrameGroupTask implements Runnable {

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

        try {
            DataChunk cdc = null;
            SQLGroup sqlg = null;
            while (((StreamQueue) target).isRunning()) {
                EntityContainer o = (EntityContainer) q.poll();
                if (o != null) {
                    if (cdc != null) {
                        if (o.getDataChunk().compare(cdc, cur.getSqlStmt().getCols().getGroupColumns().size()) == 0) { //cdc & c chunks grouped
                            sqlg.add(o.getDataChunk());
                        } else { // start next group
                            DataChunk gdc = sqlg.getDC();
                            Object oo = gdc.getEntity(gtable);
                            target.persist(oo, s);
                            sqlg = new SQLGroup(o.getDataChunk(), cur.getSqlStmt().getCols().getColumns());
                            sqlg.add(o.getDataChunk());
                        }
                    } else {
                        sqlg = new SQLGroup(o.getDataChunk(), cur.getSqlStmt().getCols().getColumns());
                        sqlg.add(o.getDataChunk());
                    }
                    cdc = o.getDataChunk();

                    if (q.peek() == null) {
                        Thread.sleep(100);
                    }
                }
            }
            DataChunk gdc = sqlg.getDC();
            Object oo = gdc.getEntity(gtable);
            target.persist(oo, s);
        } catch (Exception e) {
            ((StreamQueue) target).stop(s);
            throw new RuntimeException(e);
        }
    }

}
