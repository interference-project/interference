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
import su.interference.core.*;
import su.interference.metrics.Metrics;
import su.interference.persistent.*;
import su.interference.proxy.GenericResult;
import su.interference.transport.SQLEvent;
import su.interference.transport.TransportContext;
import su.interference.transport.TransportEvent;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.ArrayList;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class FrameJoinTask implements Callable<List<Object>> {

    private final static Logger logger = LoggerFactory.getLogger(FrameJoinTask.class);
    private final Cursor cur;
    private final FrameApi bd1;
    private final FrameApi bd2;
    private final ResultSet target;
    private final ArrayList<SQLColumn> cols;
    private final NestedCondition nc;
    private final Session s;
    private final int nodeId;
    private final List<Object> res;
    private final String taskName;
    private final int sqlcid;
    private final boolean last;
    private final boolean leftfs;
    private final static ConcurrentHashMap<String, Class> cache = new ConcurrentHashMap<String, Class>();
    private final SQLJoinDispatcher hmap;

    static {
        cache.put("int", int.class);
        cache.put("long", long.class);
        cache.put("float", float.class);
        cache.put("double", double.class);
    }

    public FrameJoinTask(Cursor cur, FrameApi bd1, FrameApi bd2, ResultSet target, ArrayList<SQLColumn> cols, NestedCondition nc, int sqlcid, int nodeId, boolean last, boolean leftfs, SQLJoinDispatcher hmap, Session s) {
        this.cur = cur;
        this.bd1 = bd1;
        this.bd2 = bd2;
        this.target = target;
        this.cols = cols;
        this.nc = nc;
        this.s = s;
        this.res = new ArrayList<Object>();
        this.taskName = bd2==null?bd1.getFrameId()+"":bd1.getFrameId()+"-"+bd2.getFrameId();
        this.sqlcid = sqlcid;
        this.nodeId = nodeId;
        this.last = last;
        this.leftfs = leftfs;
        this.hmap = hmap;
    }

    public List<Object> call() throws Exception {
        final Thread thread = Thread.currentThread();
        thread.setName("SQL join task " + thread.getId());
        final Class r = target.getClass().getName().equals("su.interference.persistent.Table")?((Table)target).getSc():null;
        final int t1 = bd1.getObjectId();
        final int t2 = bd2==null?0:bd2.getObjectId();
        final Class c1 = Instance.getInstance().getTableById(t1).getTableClass();
        final Class c2 = bd2==null?null:Instance.getInstance().getTableById(t2).getTableClass();
        ResultSetEntity rsa = (ResultSetEntity)c1.getAnnotation(ResultSetEntity.class);
        final boolean c1rs = rsa!=null?true:false;

/*
        if (cur.getType() == Cursor.MASTER_TYPE && nodeId != Config.getConfig().LOCAL_NODE_ID && this.leftfs) {
            Metrics.get("remoteTask").start();
            final TransportEvent transportEvent = new SQLEvent(nodeId, cur.getCursorId(), bd1.getAllocId(), bd2==null?0:bd2.getAllocId(), bd2==null?null:bd2.getClass().getSimpleName(), 0, null, null, 0, true);
            TransportContext.getInstance().send(transportEvent);
            transportEvent.getLatch().await();
            Metrics.get("remoteTask").stop();
            if (!transportEvent.isFail()) {
                final List<Object> rs = ((SQLEvent) transportEvent).getCallback().getResult().getResultSet();
                Metrics.get("recordRCount").put(rs.size());
                // logger.info(rs.size()+" records returns from node " + nodeId);
                return rs;
            }
        }
*/

        final ArrayList<Object> drs1 = bd1.getFrameEntities(s);
        final ArrayList<Object> drs2 = bd2==null?null:bd2.getFrameEntities(s);

        if (bd2 != null && bd2.getImpl() == FrameApi.IMPL_INDEX && bd1.getImpl() == FrameApi.IMPL_INDEX) {
            if (((SQLIndexFrame) bd2).isMerged()) {
                final int s1 = drs1.size();
                final int s2 = drs2.size();
                int p1 = 0;
                int p2 = 0;
                boolean cnue = true;
                while (cnue) {
                    if (s1 > 0 && s2 > 0) {
                        final Comparable o1 = getKeyValue(drs1.get(p1).getClass(), drs1.get(p1), ((SQLIndexFrame)bd2).getLkey(), s);
                        final Comparable o2 = getKeyValue(drs2.get(p2).getClass(), drs2.get(p2), ((SQLIndexFrame)bd2).getRkey(), s);
                        final int cmp = o1.compareTo(o2);
                        if (cmp < 0) {
                            p1++;
                        } else if (cmp > 0) {
                            p2++;
                        } else {
                            Object j = null;
                            final IndexChunk ib1 = (IndexChunk) drs1.get(p1);
                            final IndexChunk ib2 = (IndexChunk) drs2.get(p2);
                            if (ib1.getDataChunk() == null) {
                                logger.error(bd1.getAllocId()+" "+ bd1.getFrameId()+" found ib1chunk = null, drs1.size = "+drs1.size()+" p1 = "+p1);
                            }
                            if (ib2.getDataChunk() == null) {
                                logger.error(bd2.getAllocId()+" "+bd2.getFrameId()+" found ib2chunk = null, drs2.size = "+drs2.size()+" p2 = "+p2);
                            }
                            final Object oo1 = ib1.getDataChunk()==null?null:ib1.getDataChunk().getEntity();
                            final Object oo2 = ib2.getDataChunk()==null?null:ib2.getDataChunk().getEntity();
                            j = joinDataRecords(r, c1, c2, t1, t2, oo1, oo2, cols, c1rs, s);
                            if (hmap.skipCheckNC()) {
                                res.add(j);
                            } else {
                                if (nc.checkNC(j, sqlcid, last)) {
                                    res.add(j);
                                }
                            }
                            final int n = p2 + 1;
                            if (n==s2) {
                                p1++;
                            } else {
                                final Comparable next = getKeyValue(drs2.get(n).getClass(), drs2.get(n), ((SQLIndexFrame)bd2).getRkey(), s);
                                if (o1.compareTo(next)<0) {
                                    p1++;
                                } else {
                                    p2++;
                                }
                            }
                        }
                        if (p1==s1||p2==s2) {
                            cnue = false;
                        }
                    } else {
                        cnue = false;
                    }
                }
            }
        } else {
            for (Object o1 : drs1) {
                if (bd1.getImpl() == FrameApi.IMPL_INDEX) {
                    final IndexChunk ib1 = (IndexChunk) o1;
                    o1 = ib1.getDataChunk().getEntity();
                }
                if (drs2==null) {
                    //HashFrame returns null drs
                    if (bd2 != null && bd2.getImpl() == FrameApi.IMPL_HASH) {
                        final Comparable key = getKeyValue(c1, o1, ((SQLHashMapFrame) bd2).getCkey(), s);
                        final List<Object> o2 = ((SQLHashMapFrame) bd2).get(key);
                        if (o2 != null) {
                            if (!(o2.get(0) == null && last)) {
                                Object j = joinDataRecords(r, c1, c2, t1, t2, o1, o2.get(0), cols, c1rs, s);
                                if (hmap.skipCheckNC()) {
                                    res.add(j);
                                } else {
                                    if (nc.checkNC(j, sqlcid, last)) {
                                        res.add(j);
                                    }
                                }
                            }
                        }
                    } else if (bd2 != null && bd2.getImpl() == FrameApi.IMPL_INDEX) {
                        final Comparable key = getKeyValue(c1, o1, ((SQLIndexFrame) bd2).getLkey(), s);
                        final List<Object> o2 = ((SQLIndexFrame) bd2).get(key);
                        //todo may cause wrong (cutted) resultsets in non-last cursors for (OR) conditions - see hashmap impl above
                        if (o2 != null) {
                            for (Object o2_ : o2) {
                                final IndexChunk ib = (IndexChunk)((DataChunk) o2_).getEntity();
                                final Object oo2 = ib.getDataChunk().getEntity();
                                final Object j = joinDataRecords(r, c1, c2, t1, t2, o1, oo2, cols, c1rs, s);
                                if (hmap.skipCheckNC()) {
                                    res.add(j);
                                } else {
                                    if (nc.checkNC(j, sqlcid, last)) {
                                        res.add(j);
                                    }
                                }
                            }
                        }
                    } else {
                        //one table loop
                        if (r == null) {
                            //todo need to cast o1 to RS type
                            //if (nc.checkNC(o1, sqlcid, last)) {
                                res.add(o1); //target table is null -> result class is null -> returns generic entities
                            //}
                        } else {
                            Object j = joinDataRecords(r, c1, c2, t1, t2, o1, null, cols, c1rs, s);
                            if (nc.checkNC(j, sqlcid, last)) {
                                res.add(j);
                            }
                        }
                    }
                } else {
                    //nested loop
                    for (Object o2 : drs2) {
                        Object j = joinDataRecords(r, c1, c2, t1, t2, o1, o2, cols, c1rs, s);
                        if (nc.checkNC(j, sqlcid, last)) {
                            res.add(j);
                        }
                    }
                }
            }
        }
        Metrics.get("recordLCount").put(res.size());
        // logger.info(res.size()+" records returns from local node " + nodeId);
        return res;
    }

    public Object joinDataRecords (Class r, Class c1, Class c2, int t1, int t2, Object o1, Object o2, ArrayList<SQLColumn> cols, boolean isrs, Session s)
        throws NoSuchMethodException, IllegalAccessException, InstantiationException, InvocationTargetException, MalformedURLException, ClassNotFoundException {
        final GenericResult ret = (GenericResult)r.newInstance();
        if (isrs) {
            for (SQLColumn sqlc : cols) {
                final Method y = c1.getMethod("get"+sqlc.getAlias().substring(0,1).toUpperCase()+sqlc.getAlias().substring(1,sqlc.getAlias().length()), null);
                final Method z = r.getMethod("set"+sqlc.getAlias().substring(0,1).toUpperCase()+sqlc.getAlias().substring(1,sqlc.getAlias().length()), new Class<?>[]{getClassByName(sqlc.getResultSetType())});
                z.invoke(ret, new Object[]{y.invoke(o1, null)});
                //fill genericResult
                //ret.setValueByName(sqlc.getAlias(), y.invoke(o1, null));
            }
            if (c2!=null) {
                for (SQLColumn sqlc : cols) {
                    if (sqlc.getObjectId() == t2) {
                        final Method y = c2.getMethod("get" + sqlc.getColumn().getName().substring(0, 1).toUpperCase() + sqlc.getColumn().getName().substring(1, sqlc.getColumn().getName().length()), new Class<?>[]{Session.class});
                        final Method z = r.getMethod("set" + sqlc.getAlias().substring(0, 1).toUpperCase() + sqlc.getAlias().substring(1, sqlc.getAlias().length()), new Class<?>[]{getClassByName(sqlc.getResultSetType())});
                        z.invoke(ret, new Object[]{y.invoke(o2, new Object[]{s})});
                        //fill genericResult
                        //ret.setValueByName(sqlc.getAlias(), y.invoke(o2, new Object[]{s}));
                    }
                }
            }
        } else {
            for (SQLColumn sqlc : cols) {
                if (sqlc.getObjectId()==t1) {
                    final Method y = c1.getMethod("get"+sqlc.getColumn().getName().substring(0,1).toUpperCase()+sqlc.getColumn().getName().substring(1,sqlc.getColumn().getName().length()), new Class<?>[]{Session.class});
                    final Method z = r.getMethod("set"+sqlc.getAlias().substring(0,1).toUpperCase()+sqlc.getAlias().substring(1,sqlc.getAlias().length()), new Class<?>[]{getClassByName(sqlc.getResultSetType())});
                    z.invoke(ret, new Object[]{y.invoke(o1, new Object[]{s})});
                    //fill genericResult
                    //ret.setValueByName(sqlc.getAlias(), y.invoke(o1, new Object[]{s}));
                }
                if (c2 != null && o2 != null) {
                    if (sqlc.getObjectId() == t2) {
                        final Method y = c2.getMethod("get" + sqlc.getColumn().getName().substring(0, 1).toUpperCase() + sqlc.getColumn().getName().substring(1, sqlc.getColumn().getName().length()), new Class<?>[]{Session.class});
                        final Method z = r.getMethod("set" + sqlc.getAlias().substring(0, 1).toUpperCase() + sqlc.getAlias().substring(1, sqlc.getAlias().length()), new Class<?>[]{getClassByName(sqlc.getResultSetType())});
                        z.invoke(ret, new Object[]{y.invoke(o2, new Object[]{s})});
                        //fill genericResult
                        //ret.setValueByName(sqlc.getAlias(), y.invoke(o2, new Object[]{s}));
                    }
                }
            }
        }

        return ret;
    }

    public Class getClassByName(String name) throws ClassNotFoundException {
        final Class c = cache.get(name);
        if (c!=null) {
            return c;
        }
        final Class lc = Class.forName(name);
        return lc;
    }

    public Comparable getKeyValue(Class c, Object o, SQLColumn sqlc, Session s) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        final Method y = sqlc.isCursor() ? c.getMethod("get" + sqlc.getColumn().getName().substring(0, 1).toUpperCase() + sqlc.getColumn().getName().substring(1, sqlc.getColumn().getName().length()), null)
                : c.getMethod("get" + sqlc.getColumn().getName().substring(0, 1).toUpperCase() + sqlc.getColumn().getName().substring(1, sqlc.getColumn().getName().length()), new Class<?>[]{Session.class});
        return sqlc.isCursor() ? (Comparable) y.invoke(o, null) : (Comparable) y.invoke(o, new Object[]{s});
    }

}
