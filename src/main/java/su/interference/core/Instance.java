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

package su.interference.core;

import su.interference.metrics.Metrics;
import su.interference.persistent.*;
import su.interference.persistent.Process;
import su.interference.exception.*;

import java.io.IOException;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.lang.reflect.InvocationTargetException;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import su.interference.transport.TransportContext;
import su.interference.transport.TransportSyncTask;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class Instance implements Interference {
    
    public static final String RELEASE = "2021.1";
    public static final int SYSTEM_VERSION = 20210412;

    public static final String DATA_FILE = "datafile";
    public static final String INDX_FILE = "indxfile";
    public static final String JRNL_FILE = "jrnlfile";
    public static final String TEMP_FILE = "tempfile";
    public static final String UNDO_FILE = "undofile";
    public static final String LOG_FILE  = "systemlog";

    public static final boolean LOG_INTERNAL_EXCEPTIONS = true;
    private static final String LOCALHOST_DEFAULT = "localhost";
    private static final int MAX_NODE_ID = 32;
    public static final int SESSION_EXPIRE = 7200000; //in ms

    public static final int SYSTEM_STATE_ONLINE = 1;
    public static final int SYSTEM_STATE_UP = 2;
    public static final int SYSTEM_STATE_FAIL = 3;
    public static final int SYSTEM_STATE_RECOVER = 4;
    public static final int SYSTEM_STATE_DOWN = 5;
    public static final int SYSTEM_STATE_IDLE = 6;
    public static final int SYSTEM_STATE_NA = 7;

    public static final int ROOT_USER_ID = 1;

    public static final int CLUSTER_STATE_UP = 1;
    public static final int CLUSTER_STATE_DOWN = 2;

    private String javaVersion;
    private String osName;
    private String osVersion;
    private String osArch;
    private String fileSeparator;

    private static Instance instance;
    private int systemState;
    private int clusterState;
    private static final URLClassLoader ucl;
    private Table tt;

    private static final Logger logger = LoggerFactory.getLogger(Instance.class);

    static {
        ucl = Instance.getUCL();
    }

    //alternative singleton holder
    private static class InstanceHolder {
        //public static Instance instance = new Instance();
    }

    private Instance() {

    }

    public boolean isStarted () throws InternalException {
        //checkInstance();
        return this.getSystemState()==SYSTEM_STATE_UP;
    }

    public boolean isStopped () throws InternalException {
        //checkInstance();
        return this.getSystemState()==SYSTEM_STATE_DOWN;
    }

    public boolean isCreated () throws InternalException {
        //checkInstance();
        return !(this.getSystemState()==SYSTEM_STATE_IDLE);
    }

    public boolean isIdle () throws InternalException {
        //checkInstance();
        return (this.getSystemState()==SYSTEM_STATE_IDLE);
    }

    public void commit (Session s) {
        s.commit();
    }

    public void rollback (Session s) {
        s.rollback();
    }

    public static boolean initParams (String[] params) {
        final Config cfg = Config.getConfig();
/*
        try {
            new HTTPServer(cfg.MMPORT);
            return true;
        } catch (IOException e) {
            return false;
        } catch (NumberFormatException e) {
            return false;
        }
*/
        return true;
    }

    private static int validateNodeId(int id) {
        if (id>=1&&id<=MAX_NODE_ID) {
            return id;
        }
        logger.error("node id "+id+" is not valid");
        throw new NumberFormatException();
    }

    private static String validatePath(String path) {
        if (path==null||path.equals("")) {
            logger.error("path '"+path+"' is not valid");
            throw new NumberFormatException();
        }
        return path;
    }

    private static int validateFrameSize(int bsize) {
        for (int bs : bss) {
            if (bs == bsize) {
                return bsize;
            }
        }
        logger.error("frame size "+bsize+" is not valid");
        throw new NumberFormatException();
    }

    private static int validatePort(int port) {
        if (port>=1&&port<=65536) {
            return port;
        }
        logger.error("port "+port+" is not valid");
        throw new NumberFormatException();
    }

    private static String validateCodePage(String cp) {
        for (String c : cps) {
            if (c.equals(cp)) {
                return cp;
            }
        }
        logger.error("codepage '"+cp+"' is not valid");
        throw new NumberFormatException();
    }

    private static String validateDateFormat(String df) {
        for (String d : dfs) {
            if (d.equals(df)) {
                return df;
            }
        }
        logger.error("dateformat '"+df+"' is not valid");
        throw new NumberFormatException();
    }

    public static URLClassLoader getUCL() {
        if (ucl==null) {
            try {
                File cp = new File(Config.getConfig().DB_PATH);
                URL[] u = new URL[]{cp.toURI().toURL()};
                return new URLClassLoader(u);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }
        return ucl;
    }

    public static synchronized Instance getInstance() {
        if (instance == null) {
            final Config cfg = Config.getConfig();
            try {
                validatePath(cfg.DB_PATH);
                validatePath(cfg.JOURNAL_PATH);
                validateNodeId(cfg.LOCAL_NODE_ID);
                validatePort(cfg.MMPORT);
                validatePort(cfg.RMPORT);
                validateFrameSize(cfg.FRAMESIZE);
                validateFrameSize(cfg.FRAMESIZE2);
                validateCodePage(cfg.CODEPAGE);
                validateDateFormat(cfg.DATEFORMAT);
            } catch (NumberFormatException e) {
                logger.info("Some startup parameters not valid, system instance cannot be initialized");
                return null;
            }

            try {
                instance = new Instance();
            } catch(Exception e) {
                e.printStackTrace();
            }

            instance.javaVersion = System.getProperty("java.version");
            instance.osName = System.getProperty("os.name");
            instance.osArch = System.getProperty("os.arch");
            instance.osVersion = System.getProperty("os.version");
            instance.fileSeparator = System.getProperty("file.separator");

            logger.info("interference "+SYSTEM_VERSION+" initializing...");
            logger.info("System state:          "+instance.systemState);
            logger.info("System database file:  "+DATA_FILE);
            logger.info("Index database file:   "+INDX_FILE);
            logger.info("Journal database file: "+JRNL_FILE);
            logger.info("Temp database file:    "+TEMP_FILE);
            logger.info("Undo database file:    "+UNDO_FILE);
            logger.info("System log file:       "+LOG_FILE);
            logger.info("Java version:          "+instance.javaVersion);
            logger.info("Operation system:      "+instance.osName);
            logger.info("Architecture:          "+instance.osArch);
            logger.info("OS vesrion:            "+instance.osVersion);
            logger.info("File separator:        "+instance.fileSeparator);
            logger.info("Processors amount      "+Runtime.getRuntime().availableProcessors());
            logger.info("System state:          "+instance.systemState);

            try {
                //node type not used this
                instance.systemState = SYSTEM_STATE_IDLE;
                instance.clusterState = cfg.CLUSTER_NODES.length==0?CLUSTER_STATE_DOWN:CLUSTER_STATE_UP;
                instance.tt = SystemInit.initSystem(false, 0, null);
                instance.systemState = ccheckInstance();
                registerMetrics();
                logger.info("Instance initialized");
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        return instance;
    }

    public int getFrameSize() throws ClassNotFoundException, InternalException, InstantiationException, IllegalAccessException {
        if (systemState==SYSTEM_STATE_IDLE) {
            return Config.getConfig().FRAMESIZE;
        }
        return Storage.getStorage().getFrameSize();
    }

    public int getFrameSize2() throws ClassNotFoundException, InternalException, InstantiationException, IllegalAccessException {
        if (systemState==SYSTEM_STATE_IDLE) {
            return Config.getConfig().FRAMESIZE2;
        }
        return Storage.getStorage().getFrameSize2();
    }

    public String getCodePage() throws ClassNotFoundException, InternalException, InstantiationException, IllegalAccessException {
        if (systemState==SYSTEM_STATE_UP) {
            return Storage.getStorage().getCodePage();
        }
        return Config.getConfig().CODEPAGE;
    }

    public String getDateFormat() throws ClassNotFoundException, InternalException, InstantiationException, IllegalAccessException {
        if (systemState==SYSTEM_STATE_UP) {
            return Storage.getStorage().getDateFormat();
        }
        return Config.getConfig().DATEFORMAT;
    }

    public String getLocalhost() {
        return LOCALHOST_DEFAULT;
    }

    public int getMMPort() throws ClassNotFoundException, InternalException, InstantiationException, IllegalAccessException {
        if (systemState==SYSTEM_STATE_IDLE) {
            return Config.getConfig().MMPORT;
        }
        return Storage.getStorage().getMMPort();
    }

    public int getRMPort() throws ClassNotFoundException, InternalException, InstantiationException, IllegalAccessException {
        if (systemState==SYSTEM_STATE_IDLE) {
            return Config.getConfig().RMPORT;
        }
        return Storage.getStorage().getMMPort2();
    }

    public int getLocalNodeId() throws ClassNotFoundException, InstantiationException, InternalException, IllegalAccessException {
        if (systemState==SYSTEM_STATE_IDLE) {
            return Config.getConfig().LOCAL_NODE_ID;
        }
        return Storage.getStorage().getLocalNodeId();
    }

    public void signinInstance(Session s) throws NoSuchMethodException, InvocationTargetException, IOException, InternalException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        s.auth();
    }

    public String createInstance(int nodeType, Session s) {
        String err = "OK";
        try {
            Storage.getStorage().createSystemDataFiles(s, null);
            Storage.getStorage().openDataFiles();
            SystemInit.initSystem(true, nodeType, s);
            Storage.getStorage().closeDataFiles();
            systemState = Instance.SYSTEM_STATE_DOWN;
        } catch (Exception e) {
            e.printStackTrace();
            err = e.getClass().getName()+" throws during createInstance";
            logger.error("", e);
        }
        return err;
    }

    public void startupInstance(Session s) throws Exception, NoSuchMethodException, IOException, InternalException, ClassNotFoundException, InstantiationException, IllegalAccessException {

        boolean ok = true;

        if (s.getUserId() != Session.ROOT_USER_ID) {
            throw new AccessDenied();
        }

        if (instance.getSystemState()==Instance.SYSTEM_STATE_IDLE) {
            instance.createInstance(Node.NODE_TYPE_MASTER, s);
        }

        if (ok) {

            logger.info("interference is starting...");
            Thread.currentThread().setName("interference-main-thread-"+Thread.currentThread().getId());
            Storage.getStorage().restoreJournal();
            Storage.getStorage().openDataFiles();
            initSystemTable();
            Storage.getStorage().closeDataFiles();
            Storage.getStorage().openStorage(getDataFiles());
            s.persist(Session.getDntmSession());
            for (String cl : Config.getConfig().REGISTER_CLASSES) {
                try {
                    s.registerTable(cl, s);
                } catch (Exception e) {
                    logger.error("Class registration failed: " + cl, e);
                }
            }
            TransportContext.getInstance().start();
            startProcesses(s);
            checkOpenTransactions(s);
            systemState = Instance.SYSTEM_STATE_UP;
            //checkInMemoryIndexes();
            logger.info("\n----------------------------------------------------------------------\n" +
                          "------------------------ interference started ------------------------\n" +
                          "------------------ (c) head systems, ltd 2010-2021 -------------------\n" +
                          "--------------------------- release "+RELEASE+" ---------------------------\n" +
                          "----------------------------------------------------------------------");
        } else {

            systemState = Instance.SYSTEM_STATE_NA;

        }

    }

    private static void registerMetrics() throws Exception {
        Metrics.register(Metrics.TIMER, "getAvailableFrame");
        Metrics.register(Metrics.TIMER, "allocateFrame");
        Metrics.register(Metrics.TIMER, "reallocateFrame");
        Metrics.register(Metrics.TIMER, "remoteTask");
        Metrics.register(Metrics.TIMER, "localTask");
        Metrics.register(Metrics.TIMER, "executeQuery");
        Metrics.register(Metrics.TIMER, "deallocateQuery");
        Metrics.register(Metrics.TIMER, "syncFrames");
        Metrics.register(Metrics.TIMER, "persistGetChunk");
        Metrics.register(Metrics.TIMER, "persistInsertChunk");
        Metrics.register(Metrics.TIMER, "persistInsertIndex");
        Metrics.register(Metrics.TIMER, "syncFrameEvent");
        Metrics.register(Metrics.HISTOGRAM, "recordRCount");
        Metrics.register(Metrics.HISTOGRAM, "recordLCount");
        Metrics.register(Metrics.HISTOGRAM, "syncQueue");
        Metrics.register(Metrics.TIMER, "systemCleanUp");
        Metrics.register(Metrics.HISTOGRAM, "сleanUpDataFrames");
        Metrics.register(Metrics.HISTOGRAM, "сleanUpIndexFrames");
        Metrics.register(Metrics.HISTOGRAM, "сleanUpUndoFrames");
        Metrics.register(Metrics.METER, "imDataFrames");
        Metrics.register(Metrics.METER, "imIndexFrames");
        Metrics.register(Metrics.METER, "imUndoFrames");
    }

    @SuppressWarnings("unchecked")
    private void checkInMemoryIndexes() {
        final MapField map = tt.getMapFieldByColumn("name");
        final Set<Map.Entry> xx = map.getMap().entrySet();
        for (Map.Entry x : xx) {
            final Table t = (Table)((DataChunk)x.getValue()).getEntity();
            t.printIndexInfo();
        }
    }

    private void checkOpenTransactions(Session s) {
        for (Transaction t : getTransactions()) {
            if (t.getCid() == 0 && t.getTransType() < Transaction.TRAN_THR) {
                logger.info("Rollback incomplete transaction id="+t.getTransId());
                t.retrieveTframes();
                if (t.isLocal()) {
                    t.rollback(s, false);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void startProcesses(Session s) throws Exception {
        final Table t = getTableByName("su.interference.persistent.Process");
        final List<Process> pss = Instance.getInstance().getProcesses();
        Collections.sort(pss);
        for (Process ps : pss) {
            final Class pc = Thread.currentThread().getContextClassLoader().loadClass(ps.getClassName());
            final Class<?>[] is = pc.getInterfaces();
            Runnable r = null;
            for (Class<?> i : is) {
                if (i.getName().equals("java.lang.Runnable")) {
                    r = (Runnable)pc.newInstance();
                }
            }
            ps.start(r, s);
        }
    }

    private void stopProcesses(Session s) throws Exception {
        final List<Process> pss = getProcesses();
        for (Object p : pss) {
            final Process ps = (Process) p;
            ps.stop();
        }
    }

    private static int ccheckInstance() throws InternalException {
        final int sstate = Storage.getStorage().getState();
        if (sstate==Storage.STORAGE_STATE_OPEN) {
            return Instance.SYSTEM_STATE_UP;
        } else if (sstate==Storage.STORAGE_STATE_CLOSED) {
            return Instance.SYSTEM_STATE_DOWN;
        } else if (sstate==Storage.STORAGE_STATE_FAIL) {
            return Instance.SYSTEM_STATE_NA;
        } else if (sstate==Storage.STORAGE_STATE_EMPTY) {
            return Instance.SYSTEM_STATE_IDLE;
        }
        return Instance.SYSTEM_STATE_NA;
    }

    public void shutdownInstance(Session s) throws Exception {
        if (s.getUserId()!=Session.ROOT_USER_ID) {
            throw new AccessDenied();
        }

        shutdownImmediate(s);
    }

    public void shutdownInstance() throws Exception {
        final Session s = Session.getDntmSession();
        shutdownImmediate(s);
    }

    private void shutdownImmediate(Session s) throws Exception {
        logger.info("Shutdown instance...");
        stopProcesses(s);
        TransportContext.getInstance().stop();
        Storage.getStorage().closeStorage(getDataFiles());
        Session.setDntmSession(s);
        systemState = Instance.SYSTEM_STATE_DOWN;
        logger.info("Instance succesfully down");
    }

    public DataFile[] getDataFiles() {
        final Table t = getTableByName("su.interference.persistent.DataFile");
        final ArrayList<DataFile> res = new ArrayList<DataFile>();
        for (Object o : t.getIndexFieldByColumn("fileId").getIndex().getContent()) {
            res.add((DataFile)((DataChunk)o).getEntity());
        }
        return res.toArray(new DataFile[]{});
    }

    public MgmtModule[] getMgmtModules() {
        final Table t = getTableByName("su.interference.persistent.MgmtModule");
        final ArrayList<MgmtModule> res = new ArrayList<MgmtModule>();
        for (Object o : t.getIndexFieldByColumn("moduleId").getIndex().getContent()) {
            res.add((MgmtModule)((DataChunk)o).getEntity());
        }
        return res.toArray(new MgmtModule[]{});
    }

    public synchronized MgmtModule getMgmtModuleById (int id) {
        final Table t = getTableByName("su.interference.persistent.MgmtModule");
        return (MgmtModule)((DataChunk)t.getIndexFieldByColumn("moduleId").getIndex().getObjectByKey(id)).getEntity();
    }

    public synchronized Cursor getCursorById (long id) {
        final Table t = getTableByName("su.interference.persistent.Cursor");
        return (Cursor)((DataChunk)t.getIndexFieldByColumn("cursorId").getIndex().getObjectByKey(id)).getEntity();
    }

    public Table getTableById (int id) {
        if (tt != null) {
            final MapField map = tt.getMapFieldByColumn("objectId");
            if (map != null) {
                final DataChunk c = (DataChunk)map.getMap().get(id);
                if (c != null) {
                    return (Table) c.getEntity();
                }
            }
        }
        return null;
    }

    public synchronized Table getTableByName (String name) {
        final MapField map = tt.getMapFieldByColumn("name");
        if (map != null && name != null) {
            final DataChunk c = (DataChunk)map.getMap().get(name);
            if (c!=null) {
                return (Table)c.getEntity();
            }
        }
        return null;
    }

    public Table getFrameDataTable() {
        return (Table)((DataChunk)tt.getMapFieldByColumn("name").getMap().get("su.interference.persistent.FrameData")).getEntity();
    }

    public Map getFramesMap () {
        final Table t = getTableByName("su.interference.persistent.FrameData");
        final MapField ixf = t.getMapFieldByColumn("frameId");
        return ixf.getMap();
    }

    public FrameData getFrameById (long id) {
        final Table t = getTableByName("su.interference.persistent.FrameData");
        final MapField ixf = t.getMapFieldByColumn("frameId");
        final Map ixl = ixf.getMap();
        final DataChunk dc = (DataChunk)ixl.get(id);
        if (dc != null) {
            return (FrameData)dc.getEntity();
        }
        return null;
    }

    public FrameData getFrameByIdForUpdate (long id, LLT llt) {
        final Table t = getTableByName("su.interference.persistent.FrameData");
        final MapField ixf = t.getMapFieldByColumn("frameId");
        final Map ixl = ixf.getMap();
        final DataChunk dc = (DataChunk)ixl.get(id);
        if (dc != null) {
            final FrameData bd = (FrameData) dc.getEntity();
            llt.add(bd);
            return bd;
        }
        return null;
    }

    public FrameData getFrameByAllocId (long id) {
        final Table t = getTableByName("su.interference.persistent.FrameData");
        final MapField ixf = t.getMapFieldByColumn("allocId");
        final Map ixl = ixf.getMap();
        final DataChunk dc = (DataChunk)ixl.get(id);
        if (dc!=null) {
            return (FrameData)dc.getEntity();
        }
        return null;
    }

    public Chunk getChunkByPointer (long frameId, int ptr) throws Exception {
        final Table t = getTableByName("su.interference.persistent.FrameData");
        final MapField ixf = t.getMapFieldByColumn("frameId");
        final Map ixl = ixf.getMap();
        final DataChunk dc = (DataChunk)ixl.get(frameId);
        if (dc!=null) {
            return ((FrameData)dc.getEntity()).getFrame().data.getByPtr(ptr);
        }
        return null;
    }

    public DataFile getDataFileById (int id) {
        final Table t = getTableByName("su.interference.persistent.DataFile");
        return (DataFile)((DataChunk)t.getIndexFieldByColumn("fileId").getIndex().getObjectByKey(id)).getEntity();
    }

    public ArrayList<DataFile> getDataFilesByType (int id) {
        final Table t = getTableByName("su.interference.persistent.DataFile");
        final ArrayList<DataFile> r = new ArrayList<DataFile>();
        for (Object o : t.getIndexFieldByColumn("type").getIndex().getObjectsByKey(id)) {
            r.add((DataFile)((DataChunk)o).getEntity());
        }
        return r;
    }

    public synchronized Session getSession (String sessionId) {
        if (Instance.getInstance().systemState==Instance.SYSTEM_STATE_UP) {
            Table t = getTableByName("su.interference.persistent.Session");
            return (Session)((DataChunk)t.getIndexFieldByColumn("sessionId").getIndex().getObjectByKey(sessionId)).getEntity();
        } else {
            return Session.getDntmSession();
        }
    }

    public synchronized Session getSessionBySid (long sid) {
        Table t = getTableByName("su.interference.persistent.Session");
        return (Session)((DataChunk)t.getIndexFieldByColumn("sid").getIndex().getObjectByKey(sid)).getEntity();
    }

    public synchronized Transaction getTransactionById (long transId) {
        if (transId == 0) { return null; }
        Table t = getTableByName("su.interference.persistent.Transaction");
        DataChunk dc = ((DataChunk)t.getIndexFieldByColumn("transId").getIndex().getObjectByKey(transId));
        if (dc==null) {
            return null;
        }
        return (Transaction)dc.getEntity();
    }

    public FreeFrame getFreeFrameById (long id) {
        final Table t = getTableByName("su.interference.persistent.FreeFrame");
        final DataChunk dc = (DataChunk)t.getIndexFieldByColumn("frameId").getIndex().getObjectByKey(id);
        if (dc==null) {
            return null;
        }
        return (FreeFrame)dc.getEntity();
    }

    public ArrayList<FrameSync> getSyncFrames(int nodeId) {
        final Table t = getTableByName("su.interference.persistent.FrameSync");
        final ArrayList<FrameSync> r = new ArrayList<>();
        String uuid = null;
        for (Object o : t.getIndexFieldByColumn("syncId").getIndex().getContent(TransportSyncTask.REMOTE_SYNC_DEFERRED_AMOUNT)) {
            final FrameSync fs = (FrameSync)((DataChunk)o).getEntity();
            if (fs.getNodeId() == nodeId) {
                if (uuid == null) {
                    uuid = fs.getSyncUUID();
                    r.add(fs);
                } else {
                    if (uuid.equals(fs.getSyncUUID())) {
                        r.add(fs);
                    }
                }
            }
        }
        return r;
    }

    public synchronized Process getProcessByName (String name) {
        final Table t = getTableByName("su.interference.persistent.Process");
        return (Process)((DataChunk)t.getIndexFieldByColumn("processName").getIndex().getObjectByKey(name)).getEntity();
    }

    public synchronized List<Process> getProcesses () {
        final Table t = getTableByName("su.interference.persistent.Process");
        final ArrayList<Process> r = new ArrayList<Process>();
        for (Object o : t.getIndexFieldByColumn("processId").getIndex().getContent()) {
            r.add((Process)((DataChunk)o).getEntity());
        }
        return r;
    }

    //used in storeFrame
    public TransFrame getTransFrameById(long transId, long cframeId, long uframe) {
        final Table t = getTableByName("su.interference.persistent.TransFrame");
        final TransFrameId id = new TransFrameId(cframeId, uframe, transId);
        DataChunk dc = (DataChunk) t.getMapFieldByColumn("frameId").getMap().get(id);
        if (dc != null) {
            return (TransFrame) dc.getEntity();
        }
        return null;
    }

    public List<TransFrame> getTransFramesByTransId(long transId) {
        List<TransFrame> res = new ArrayList<>();
        final Table t = getTableByName("su.interference.persistent.TransFrame");
        for (Map.Entry entry : ((Map<Object, Object>)t.getMapFieldByColumn("frameId").getMap()).entrySet()) {
            final TransFrame tf = (TransFrame) ((DataChunk) entry.getValue()).getEntity();
            if (tf.getTransId() == transId) {
                res.add(tf);
            }
        }
        return res;
    }

    //used in unlock table mechanism
    @Deprecated
    public synchronized ArrayList<TransFrame> getTransFrameByObjectId (int objectId) {
        //final Table t = getTableByName("su.interference.persistent.TransFrame");
        return new ArrayList<TransFrame>();
    }

    public RetrieveLock getRetrieveLockById(RetrieveLock p) {
        final Table t = getTableByName("su.interference.persistent.RetrieveLock");
        for (Object o : t.getIndexFieldByColumn("objectId").getIndex().getObjectsByKey(p.getObjectId())) {
            final RetrieveLock rl = (RetrieveLock)((DataChunk)o).getEntity();
            if (rl.getTransId()==p.getTransId()) {
                return rl;
            }
        }
        return null;
    }

    public RetrieveLock getRetrieveLockById(int obj, long tran) {
        final Table t = getTableByName("su.interference.persistent.RetrieveLock");
        for (Object o : t.getIndexFieldByColumn("objectId").getIndex().getObjectsByKey(obj)) {
            final RetrieveLock rl = (RetrieveLock)((DataChunk)o).getEntity();
            if (rl.getTransId()==tran) {
                return rl;
            }
        }
        return null;
    }

    public ArrayList<RetrieveLock> getRetrieveLocksByObjectId(int obj) {
        final ArrayList<RetrieveLock> r = new ArrayList<RetrieveLock>();
        final Table t = getTableByName("su.interference.persistent.RetrieveLock");
        for (Object o : t.getIndexFieldByColumn("objectId").getIndex().getObjectsByKey(obj)) {
            final RetrieveLock rl = (RetrieveLock)((DataChunk)o).getEntity();
            r.add(rl);
        }
        return r;
    }

    public synchronized List<Transaction> getTransactions() {
        final Table t = getTableByName("su.interference.persistent.Transaction");
        final ArrayList<Transaction> res = new ArrayList<Transaction>();
        for (Object o : t.getIndexFieldByColumn("transId").getIndex().getContent()) {
            res.add((Transaction)((DataChunk)o).getEntity());
        }
        return res;
    }

    private synchronized void initSystemTable () throws Exception {
        this.tt = Storage.getStorage().bootstrapLoad();
    }

    public long getFreeMemory() {
        return Runtime.getRuntime().freeMemory()/1048576;
    }

    public long getMaxMemory() {
        return Runtime.getRuntime().maxMemory()/1048576;
    }

    public long getTotalMemory() {
        return Runtime.getRuntime().totalMemory()/1048576;
    }

    public int getSystemState() {
        return systemState;
    }

    public int getClusterState() {
        return clusterState;
    }

    public String getJavaVersion() {
        return javaVersion;
    }

    public String getOsName() {
        return osName;
    }

    public String getOsVersion() {
        return osVersion;
    }

    public String getOsArch() {
        return osArch;
    }

    public String getFileSeparator() {
        return fileSeparator;
    }

    public static final String[] cps = new String[]{"Cp858","Cp437","Cp775","Cp850","Cp852","Cp855","Cp857","Cp862","Cp866","ISO8859_1","ISO8859_2","ISO8859_4","ISO8859_5","ISO8859_7",
                         "ISO8859_9","ISO8859_13","ISO8859_15","KOI8_R","KOI8_U","ASCII","UTF8","UTF-16","UnicodeBigUnmarked","UnicodeLittleUnmarked","UTF_32","UTF_32BE","UTF_32LE",
                         "UTF_32BE_BOM","UTF_32LE_BOM","Cp1250","Cp1251","Cp1252","Cp1253","Cp1254","Cp1257","UnicodeBig","Cp737","Cp874","UnicodeLittle"};

    public static final String[] dfs = new String[]{"dd.MM.yyyy", "dd.MM.yyyy HH:mm", "dd.MM.yyyy HH:mm:ss",
                                                    "MM.dd.yyyy", "MM.dd.yyyy HH:mm", "MM.dd.yyyy HH:mm:ss",
                                                    "yyyy.MM.dd", "yyyy.MM.dd HH:mm", "yyyy.MM.dd HH:mm:ss"};

    public static final int[] bss = new int[]{4096, 8192, 16384, 32768, 65536, 131072, 262044, 524288};
}
