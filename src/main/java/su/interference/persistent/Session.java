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

package su.interference.persistent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.core.MapField;
import su.interference.core.*;
import su.interference.exception.*;
import su.interference.mgmt.MgmtColumn;
import su.interference.proxy.*;
import su.interference.sql.ResultSet;
import su.interference.sql.SQLColumn;
import su.interference.sql.SQLCursor;
import su.interference.sql.SQLSelect;
import su.interference.transport.RemoteSession;
import su.interference.transport.TransportChannel;

import java.lang.reflect.Field;
import java.util.*;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.concurrent.*;
import javax.persistence.*;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

@Entity
@SystemEntity
@DisableSync
public class Session implements OnDelete {
    @Transient
    public static final int ROOT_USER_ID = 1;

    @Id
    @GeneratedValue
    @DistributedId
    @Column
    @IndexColumn
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private long sid;
    @Column
    @IndexColumn
    @MgmtColumn(width=10, show=true, form=true, edit=false)
    private String sessionId;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int nodeId;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int userId;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private Date dateStart;
    @Column
    private Date dateEnd;
    @Column
    private Date dateLastAction;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private String ipAddress;

    @Transient
    private Transaction transaction;

    //store params from webform for auth session
    @Transient
    private int user;
    @Transient
    private int pass;
    @Transient
    private static Session dntmSession;
    @Transient
    public static final int CLASS_ID = 6;
    @Transient
    private final static Logger logger = LoggerFactory.getLogger(Session.class);

    public static int getCLASS_ID() {
        return CLASS_ID;
    }

    @Transient
    private static ThreadLocal<Session> contextSession = new ThreadLocal<Session>();
    @Transient
    private final TransportChannel sessionChannel;

    @Transient
    private static final ExecutorService rqpool = Executors.newCachedThreadPool();
    @Transient
    private volatile Map<Integer, RetrieveQueue> retrieveQueue = new ConcurrentHashMap<>();
    @Transient
    private static final ExecutorService streampool = Executors.newCachedThreadPool();
    @Transient
    private final Map<Long, Integer> streammap = new ConcurrentHashMap<>();
    @Transient
    private final Map<String, ResultSet> rss = new ConcurrentHashMap<>();
    @Transient
    private volatile boolean stream;

    public static Session getContextSession() {
        return contextSession.get();
    }

    protected static void setContextSession(Session s) {
        Session.contextSession.set(s);
    }

    public Transaction getTransaction() {
        try {
            if (Instance.getInstance().getSystemState()==Instance.SYSTEM_STATE_UP) {
                return transaction;
            }
        } catch (Exception e) {
            e.printStackTrace(); 
        }
        return null;
    }

    protected Transaction createTransaction(long tranId, LLT llt) {
        try {
            if (Instance.getInstance().getSystemState()==Instance.SYSTEM_STATE_UP) {
                if (transaction==null) {
                    transaction = new Transaction();
                    transaction.setSid(this.getSid());
                    transaction.setTransId(tranId);
                    this.persist(transaction, llt);
                }
                return transaction;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void startStatement() throws Exception {
        if (this.getTransaction()==null) { this.createTransaction(0, null); }
        this.getTransaction().startStatement(this);
    }

    protected void startStatement(long tranId) throws Exception {
        if (this.getTransaction()==null) { this.createTransaction(tranId, null); }
        this.getTransaction().startStatement(this);
    }

    private void startStatement(LLT llt) throws Exception {
        //if (this.getTransaction()==null) { this.createTransaction(0, llt); }
        //this.getTransaction().startStatement(this, llt);
    }

    protected void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    public Table registerTable (String n, Session s) throws Exception {
        return registerTable (n, null, s, null, null, null, false);
    }

    @SuppressWarnings("unchecked")
    public Table registerTable (String n, ClassContainer source, Session s, List<SQLColumn> cols, java.lang.reflect.Field[] flds, Table pt, boolean ixflag) throws Exception {
        final ClassLoader cl = this.getClass().getClassLoader();
        final POJOProxyFactory ppf = POJOProxyFactory.getInstance();
        final SimplePOJOProxyFactory spf = SimplePOJOProxyFactory.getInstance();
        final RSProxyFactory rpf = RSProxyFactory.getInstance();
        final IOTProxyFactory ipf = IOTProxyFactory.getInstance();

        if (source == null && cols==null && flds==null) {
            Class c = cl.loadClass(n); //try load specific class
        }

        if (source != null) {
            try {
                Class c = cl.loadClass(n); //try load specific class
                if (c != null) {
                    throw new RuntimeException("external class already registered: "+c.getName());
                }
            } catch (ClassNotFoundException e) {
                logger.debug("external class not found, ok");
            }
        }

        final Table t = Instance.getInstance().getTableByName("su.interference.persistent.Table");
        Table w = Instance.getInstance().getTableByName(n);

        if (w != null) {
            if (cols == null && flds == null) {
                if (source != null) {
                    throw new RuntimeException("external class already registered: "+ w.getName());
                } else {
                    logger.info("register table rejected: table " + n + " already registered");
                    return w;
                }
            } else {
                throw new InternalException();
            }
        }

        final Class proxy = cols==null&&flds==null?source==null?ppf.register(n):spf.compile(source):flds==null?rpf.register(cols,n,ixflag):ipf.register(flds,n,pt.getSc().getName());
        final String sname = proxy==null?null:proxy.getName();

        boolean setlbs = true;
        int framesize = Instance.getInstance().getFrameSize();
        DataFile[] datafs = Storage.getStorage().getInitDataFiles();

        if (cols!=null) {
            datafs = Storage.getStorage().getInitTempFiles();
        }

        if (flds!=null&&pt!=null) {
            datafs = Storage.getStorage().getIndexFiles();
            framesize = Instance.getInstance().getFrameSize2();
            setlbs = false;
        }

        w = (Table)t.newInstance(new Object[]{n, proxy==null?new String("x"):proxy});
        w.setFrameSize(framesize);
        w.setParentId(pt==null?0:pt.getObjectId());
        w.setProxy(source == null ? 0 : 1);
        s.persist(w); //ident

        final LLT llt = LLT.getLLT();

        for (DataFile df : datafs) {
            FrameData nb = w.createNewFrame(null, null, df.getFileId(), pt==null&&!ixflag?0:1, 0, false, setlbs, false, s, llt);
        }

        llt.commit();

        s.persist(w); //update

        if (sname != null) {
            if (cols == null && flds == null) { //add second index name for EntityContainer objects
                final MapField map = t.getMapFieldByColumn("name");
                final DataChunk dc = (DataChunk)map.getMap().get(w.getName());
                map.getMap().put(sname, dc);
            }
        }

        //create indexes
        if (pt == null) {
            final IndexDescript[] idcs = w.getIndexNames();
            for (IndexDescript ids : idcs) {
                registerTable(Table.SYSTEM_PKG_PREFIX + ids.getName(), null, s, null, ids.getFields(), w, false);
            }
        }

        logger.info("table "+w.getName()+" successfully registered with id = "+w.getObjectId());
        //init in-memory indexes for Id and Index columns of temporary table
        w.initIndexFields();
        return w;
    }

    public Object newEntity (Class c) {
        final Table t = Instance.getInstance().getTableByName(c.getName());
        try {
            return t.isNoTran() ? t.newInstance() : t.newInstance(this);
        } catch (Exception e) {
            logger.error("proxy instantiation fails", e);
        }
        return null;
    }

    public Object newEntity (Object o) {
        final Table t = Instance.getInstance().getTableByName(o.getClass().getName());
        try {
            final Object entityContainer = t.isNoTran() ? t.newInstance() : t.newInstance(this);
            final Field[] cs = t.getFields();
            for (int i=0; i<cs.length; i++) {
                Transient ta = cs[i].getAnnotation(Transient.class);
                if (ta == null) {
                    final int m = cs[i].getModifiers();
                    if (Modifier.isPrivate(m)) {
                        cs[i].setAccessible(true);
                    }
                    cs[i].set(entityContainer, cs[i].get(o));
                }
            }
            return entityContainer;
        } catch (Exception e) {
            logger.error("proxy instantiation fails", e);
        }
        return null;
    }

    public Object newEntity (Class c, Object[] params) {
        final Table t = Instance.getInstance().getTableByName(c.getName());
        try {
            return t.isNoTran() ? t.newInstance(params) : t.newInstance(params, this);
        } catch (Exception e) {
            logger.error("proxy instantiation fails", e);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public EntityFactory getEntityFactory(Class c) {
        return new EntityFactory(c, this);
    }

    public Object find (Class c, long id) throws Exception {
        return find(c.getName(), id);
    }

    public Object find (String c, long id) throws Exception {
        final Table t = Instance.getInstance().getTableByName(c);
        t.getFindMeter().start();
        if (t != null) {
            this.startStatement();
            final DataChunk dc = t.getChunkById(id, this);
            final Object res = dc == null ? null : ((EntityContainer) dc.getStandaloneEntity()).getEntity(this);
            t.getFindMeter().stop();
            return res;
        }
        t.getFindMeter().stop();
        return null;
    }

    // find method for internal system mechanism
    public Object find_ (Class c, long id) throws Exception {
        final Table t = Instance.getInstance().getTableByName(c.getName());
        if (t != null) {
            final DataChunk dc = t.getChunkById(id, this);
            return dc == null ? null : dc.getEntity();
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public synchronized RetrieveQueue getContentQueue(Table t) {
        if (t != null) {
            try {
                if (retrieveQueue.get(t.getObjectId()) == null || !retrieveQueue.get(t.getObjectId()).isRetrieve()) {
                    final RetrieveQueue rq = t.getContentQueue(this);
                    Future f = rqpool.submit(rq.getR());
                    retrieveQueue.put(t.getObjectId(), rq);
                }
                return retrieveQueue.get(t.getObjectId());
            } catch (Exception e) {
                if (e instanceof ExecutionException) {
                    e.getCause().printStackTrace();
                } else {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    public void closeQueue(int objectId) {
        if (this.retrieveQueue.get(objectId) != null) {
            retrieveQueue.get(objectId).stop();
        }
    }

    public void closeStreamQueue() {
        logger.info("Stream is closed");
        SQLCursor.removeStreamQueue(this);
    }

    public void close() {
        rqpool.shutdownNow();
        streampool.isShutdown();
    }

    //todo uncommitted data not retrieved
    @Deprecated
    public void stream (Class c, StreamCallable task) {
        final Map<Long, Long> retrieved = new HashMap<>();
        final Table t = Instance.getInstance().getTableByName(c.getName());
        final Session s = this;
        streampool.submit(new Runnable() {
            boolean running = true;
            @Override
            public void run() {
                try {
                    while (running) {
                        List<FrameData> frames = t.getStream(retrieved, s);
                        for (FrameData b : frames) {
                            for (Chunk c : b.getDataFrame().getChunks()) {
                                Object o = c.getEntity(s);
                                task.call(o);
                            }
                            retrieved.put(b.getFrameId(), b.getAllocId());
                        }
                    }
                    Thread.sleep(100);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Deprecated
    public Object nfind (Class c, long id) throws Exception {
        final Table t = Instance.getInstance().getTableByName(c.getName());
        if (t != null) {
            return t.getChunkById(id, this).getEntity(this);
        }
        return null;
    }

    public void lock (Object o) throws Exception {
        final DataChunk dc = ((EntityContainer)o).getDataChunk();
        if (dc != null) {
            dc.lock(this, null);
        }
    }

    public DataChunk persist (Object o) throws Exception {
        return persist(o, null);
    }

    public DataChunk persist (Object o, LLT llt) throws Exception {
        final String cname = o.getClass().getName();
        final Table t = Instance.getInstance().getTableByName(cname);
        if (!t.isNoTran()) {
            t.getPersistMeter().start();
        }
        if (t != null) {
            final DataChunk dc = t.persist(o, this, llt);
            if (!t.isNoTran()) {
                t.getPersistMeter().stop();
            }
            return dc;
        }
        if (!t.isNoTran()) {
            t.getPersistMeter().stop();
        }
        return null;
    }

    public void delete (Object o) throws Exception {
        delete(o, null);
    }

    public ResultSet execute(String sql) throws Exception {
        startStatement();
        final SQLSelect stmt = new SQLSelect(sql, this);
        final DataSet ds = stmt.executeSQL(this);
        return ds.getTable();
    }

    public void delete (Object o, LLT llt) throws Exception {
        final Table t = Instance.getInstance().getTableByName(o.getClass().getName());
        if (t != null) {
            t.delete(o, this, llt, false, false);
        }
        o = null;
    }

    public void purge (Object o, LLT llt) throws Exception {
        if (o instanceof EntityContainer) {
            final Table t = Instance.getInstance().getTableByName(o.getClass().getName());
            if (t != null) {
                t.delete(o, this, llt, true, false);
            }
        } else {
            throw new RuntimeException("Can't purge non-entity object");
        }
        o = null;
    }

    public void commit () {
        if (this.transaction!=null) {
            this.transaction.commit(this, false);
            this.transaction = null;
        }
    }

    public void rollback () {
        if (this.transaction!=null) {
            this.transaction.rollback(this, false);
            this.transaction = null;
        }
    }

    public void auth () throws NoSuchMethodException, InvocationTargetException, IOException, InternalException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        if (this.user >0 && this.pass > 0) {
            if (Instance.getInstance().getSystemState()==Instance.SYSTEM_STATE_UP) {
                this.userId = Storage.getStorage().auth(user, pass, this);
            } else {
                this.userId = Storage.getStorage().auth(user, pass, this);
            }
        }
    }

    public long getId () {
        return this.sid;
    }

    public Session () {
        this.sessionId = UUID.randomUUID().toString();
        this.userId = 0;
        this.dateStart = new Date();
        this.sessionChannel = null;
    }

    public Session (TransportChannel channel) {
        this.sessionId = UUID.randomUUID().toString();
        this.userId = 0;
        this.dateStart = new Date();
        this.sessionChannel = channel;
    }

    public Session (String ipAddress) {
        this.sessionId = UUID.randomUUID().toString();
        this.userId = 0;
        this.dateStart = new Date();
        this.ipAddress = ipAddress;
        this.sessionChannel = null;
    }

    public Session (int user, int pass) {
        this.sessionId = UUID.randomUUID().toString();
        this.userId = 0;
        this.dateStart = new Date();
        this.user = user;
        this.pass = pass;
        this.sessionChannel = null;
    }

    public static synchronized Session getSession () {
        final Session s = new Session();
        if (Instance.getInstance().getSystemState() == Instance.SYSTEM_STATE_UP) {
            try {
                s.persist(s); //insert
                contextSession.set(s);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            if (dntmSession==null) {
                dntmSession = s;
            }
        }
        return s;
    }

    public static synchronized Session getSession (TransportChannel channel) throws Exception {
        final Session s = new Session(channel);
        s.persist(s);
        return s;
    }

    public static synchronized RemoteSession getRemoteSession (String host, int port, String callbackHost, int callbackPort) throws InterruptedException {
        return new RemoteSession(host, port, callbackHost, callbackPort);
    }

    //constructor for low-level storage function (initial first-time load table descriptions from datafile)
    public Session (DataChunk chunk) throws IllegalAccessException, InternalException {
        this.sessionChannel = null;
        final Object[] dcs = chunk.getDcs().getValueSet();
        final Class c = this.getClass();
        final java.lang.reflect.Field[] f = c.getDeclaredFields();
        int x = 0;
        for (int i=0; i<f.length; i++) {
            final Transient ta = f[i].getAnnotation(Transient.class);
            if (ta == null) {
                final int m = f[i].getModifiers();
                if (Modifier.isPrivate(m)) {
                    f[i].setAccessible(true);
                }
                f[i].set(this, dcs[x]);
                x++;
            }
        }
    }

    public void onDelete() throws Exception {
        List<Transaction> trans = Instance.getInstance().getTransactionsBySid(this.sid);
        for (Transaction tran : trans) {
            this.delete(tran);
        }
    }

    public long getSid() {
        return sid;
    }

    public void setSid(long sid) {
        this.sid = sid;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public Date getDateStart() {
        return dateStart;
    }

    public void setDateStart(Date dateStart) {
        this.dateStart = dateStart;
    }

    public Date getDateEnd() {
        return dateEnd;
    }

    public void setDateEnd(Date dateEnd) {
        this.dateEnd = dateEnd;
    }

    public Date getDateLastAction() {
        return dateLastAction;
    }

    public void setDateLastAction(Date dateLastAction) {
        this.dateLastAction = dateLastAction;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public int getUser() { return user; }

    public void setUser(String user) {
        this.user = user.hashCode();
    }

    public int getPass() {
        return pass;
    }

    public void setPass(String pass) {
        this.pass = pass.hashCode();
    }

    public static Session getDntmSession() {
        return dntmSession;
    }

    public static void setDntmSession(Session dntmSession) {
        logger.info("set downtime session with id = "+dntmSession.getSessionId());
        Session.dntmSession = dntmSession;
    }

    public RetrieveQueue getRetrieveQueue(int objectId) {
        return retrieveQueue.get(objectId);
    }

    public void streamFramePtr(Frame f, int ptr) {
        streammap.put(f.getAllocFile()+f.getAllocPointer(), ptr);
    }

    public int streamFramePtr(Frame f) {
        return streammap.get(f.getAllocFile()+f.getAllocPointer()) == null ? 0 : streammap.get(f.getAllocFile()+f.getAllocPointer());
    }

    public void putResultSet(String uuid, ResultSet rs) {
        rss.put(uuid, rs);
    }

    public ResultSet getResultSet(String uuid) {
        return rss.get(uuid);
    }

    public boolean isStream() {
        return stream;
    }

    public void setStream(boolean stream) {
        this.stream = stream;
    }

    public TransportChannel getSessionChannel() {
        return sessionChannel;
    }
}
