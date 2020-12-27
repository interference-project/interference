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

package su.interference.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.exception.EmptyFrameHeaderFound;
import su.interference.exception.InternalException;
import su.interference.exception.InvalidFrame;
import su.interference.persistent.FrameData;
import su.interference.persistent.DataFile;
import su.interference.persistent.Session;
import su.interference.persistent.Table;
import su.interference.proxy.POJOProxyFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

@SuppressWarnings("unchecked")
public class Storage {

    //all open registered datafiles
    private HashMap<Integer, DataFile> dfs;
    //datafiles list for system init
    private final HashMap<Integer, DataFile> ifs;
    private RandomAccessFile jrnFile;
    private RandomAccessFile errFile;
    private final AtomicLong jrnptr = new AtomicLong();

    public final int INITFILE_ID;
    public final int UNDOFILE_ID;
    public final int TEMPFILE_ID;

    public static final int MAX_NODES = 64;

    public static final int DATAFILE_TYPEID = 1;
    public static final int INDXFILE_TYPEID = 2;
    public static final int UNDOFILE_TYPEID = 3;
    public static final int TEMPFILE_TYPEID = 4;

    public static final String DATA_FILE = "datafile";
    public static final String INDX_FILE = "indxfile";
    public static final String UNDO_FILE = "undofile";
    public static final String TEMP_FILE = "tempfile";

    public static final FileType[] filetypes = new FileType[4];

    public static final int STORAGE_STATE_CLOSED = 0;
    public static final int STORAGE_STATE_OPEN   = 1;
    public static final int STORAGE_STATE_FAIL   = 2;
    public static final int STORAGE_STATE_EMPTY  = 3;

    private static Storage storage;
    private int state;
    private static final Logger logger = LoggerFactory.getLogger(Storage.class);

    static {
        filetypes[0] = new FileType(DATAFILE_TYPEID, DATA_FILE);
        filetypes[1] = new FileType(INDXFILE_TYPEID, INDX_FILE);
        filetypes[2] = new FileType(UNDOFILE_TYPEID, UNDO_FILE);
        filetypes[3] = new FileType(TEMPFILE_TYPEID, TEMP_FILE);
    }

    public int getState() {
        return state;
    }

    public HashMap<Integer, DataFile> getInitFiles () {
        return ifs;
    }

    public DataFile[] getInitFilesAsArray () {
        DataFile[] res = new DataFile[ifs.size()+1];
        int cnt = 1;
        for (Map.Entry e : ifs.entrySet()) {
            res[cnt] = (DataFile)e.getValue();
            cnt++;
        }
        return res;
    }

    public HashMap<Integer, DataFile> getFiles () {
        return dfs;
    }

    public static int getIdIncrement() {
        return (Config.getConfig().LOCAL_NODE_ID-1)*MAX_NODES;
    }

    private Storage() {
        //index 0 not used
        ifs = new HashMap<Integer, DataFile>();
        INITFILE_ID = ((DATAFILE_TYPEID-1)*Config.getConfig().FILES_AMOUNT) + 1 + getIdIncrement();
        UNDOFILE_ID = ((UNDOFILE_TYPEID-1)*Config.getConfig().FILES_AMOUNT) + 1 + getIdIncrement();
        TEMPFILE_ID = ((TEMPFILE_TYPEID-1)*Config.getConfig().FILES_AMOUNT) + 1 + getIdIncrement();

        for (int i=0; i<filetypes.length; i++) {
            for (int j = 0; j<Config.getConfig().FILES_AMOUNT; j++) {
                final int id = (i*Config.getConfig().FILES_AMOUNT) + j + 1 + getIdIncrement();
                ifs.put(id, new DataFile(id, filetypes[i].getType(), Config.getConfig().DB_PATH + Instance.getInstance().getFileSeparator() + filetypes[i].getName()+j));
            }
        }
        int c = 0;
        try {
            for (Map.Entry e : ifs.entrySet()) {
                if (((DataFile)e.getValue()).checkFile()==0) {
                    c = 0;
                    break;
                } else {
                    c = 1;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (c==0) {
            state = STORAGE_STATE_EMPTY;
        }
        if (c==1) {
            state = STORAGE_STATE_CLOSED;
        }
        if (c>1) {
            state = STORAGE_STATE_FAIL;
        }
    }

    public static Storage getStorage() {
        if (storage==null) {
            storage = new Storage();
        }
        return storage;
    }

    protected void clearJournal() throws IOException {
        if (jrnFile.length()>0) {
            jrnFile.setLength(0);
            jrnptr.set(0);
        }
    }

    protected void restoreJournal() throws IOException, InternalException, IllegalAccessException, InstantiationException, ClassNotFoundException {
        final int framesize = Config.getConfig().FRAMESIZE;
        if (state == STORAGE_STATE_CLOSED) {
            if (jrnFile == null) {
                jrnFile = new RandomAccessFile(Config.getConfig().JOURNAL_PATH + Instance.getInstance().getFileSeparator() + Instance.JRNL_FILE, Config.getConfig().DISKIO_MODE);
                if (jrnFile.length()>0) {
                    if (jrnFile.length()<framesize) {
                        throw new InternalException();
                    }
                    if (jrnFile.length()%framesize>0) {
                        throw new InternalException();
                    }
                    final long amt = jrnFile.length()/framesize;
                    for (long i=0; i<amt; i++) {
                        final long ptr = i*framesize;
                        jrnFile.seek(ptr);
                        byte[] b = new byte[framesize];
                        jrnFile.read(b, 0, framesize);
                        Frame frame = new Frame(b);
                        writeFrame(b, frame.getPtr());
                    }
                }
                clearJournal();
            }
        }
    }

    public int getFrameSize() throws ClassNotFoundException, InternalException, IllegalAccessException, InstantiationException {
        final SystemData sd = getDataFileById(INITFILE_ID).getSframe().getSystemData();
        return sd.getFramesize();
    }

    public int getLocalNodeId() throws ClassNotFoundException, InternalException, IllegalAccessException, InstantiationException {
        return getDataFileById(INITFILE_ID).getSframe().getSystemData().getNodeId();
    }

    public int getFrameSize2() throws ClassNotFoundException, InternalException, IllegalAccessException, InstantiationException {
        return getDataFileById(INITFILE_ID).getSframe().getSystemData().getFramesize2();
    }

    public String getCodePage() throws ClassNotFoundException, InternalException, IllegalAccessException, InstantiationException {
        return getDataFileById(INITFILE_ID).getSframe().getSystemData().getCp();
    }

    public String getDateFormat() throws ClassNotFoundException, InternalException, IllegalAccessException, InstantiationException {
        return getDataFileById(INITFILE_ID).getSframe().getSystemData().getDf();
    }

    public int getUser() throws ClassNotFoundException, InternalException, IllegalAccessException, InstantiationException {
        return getDataFileById(INITFILE_ID).getSframe().getSystemData().getUser();
    }

    public int getPass() throws ClassNotFoundException, InternalException, IllegalAccessException, InstantiationException {
        return getDataFileById(INITFILE_ID).getSframe().getSystemData().getPasswd();
    }

    public int getMMPort() throws ClassNotFoundException, InternalException, IllegalAccessException, InstantiationException {
        final SystemData sd = getDataFileById(INITFILE_ID).getSframe().getSystemData();
        return sd.getMmport();
    }

    public int getMMPort2() throws ClassNotFoundException, InternalException, IllegalAccessException, InstantiationException {
        final SystemData sd = getDataFileById(INITFILE_ID).getSframe().getSystemData();
        return sd.getMmport2();
    }

    public int getFilesAmount() throws ClassNotFoundException, InternalException, IllegalAccessException, InstantiationException {
        final SystemData sd = getDataFileById(INITFILE_ID).getSframe().getSystemData();
        return sd.getDatafiles();
    }

    public int auth (int user, int pass, Session s) throws NoSuchMethodException, InvocationTargetException, IOException, InternalException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        return getDataFileById(INITFILE_ID).getSframe().auth(user, pass, s);
    }

    public DataFile getDataFileById(int id) throws ClassNotFoundException, InternalException, InstantiationException, IllegalAccessException {
        if (state == STORAGE_STATE_OPEN) {
            return dfs.get(id);
        } else {
            return ifs.get(id);
        }
    }

    public DataFile[] getInitDataFiles() throws ClassNotFoundException, InternalException, InstantiationException, IllegalAccessException {
        final List<DataFile> res = new ArrayList<DataFile>();
        if (state == STORAGE_STATE_OPEN) {
            for (Map.Entry e : dfs.entrySet()) {
                if (((DataFile)e.getValue()).getType()==DATAFILE_TYPEID) {
                    res.add((DataFile)e.getValue());
                }
            }
        } else {
            for (Map.Entry e : ifs.entrySet()) {
                if (((DataFile)e.getValue()).getType()==DATAFILE_TYPEID) {
                    res.add((DataFile)e.getValue());
                }
            }
        }
        return res.toArray(new DataFile[]{});
    }

    public DataFile[] getInitTempFiles() {
        final List<DataFile> res = new ArrayList<DataFile>();
        if (state == STORAGE_STATE_OPEN) {
            for (Map.Entry e : dfs.entrySet()) {
                if (((DataFile)e.getValue()).getType()==TEMPFILE_TYPEID) {
                    res.add((DataFile)e.getValue());
                }
            }
        } else {
            for (Map.Entry e : ifs.entrySet()) {
                if (((DataFile)e.getValue()).getType()==TEMPFILE_TYPEID) {
                    res.add((DataFile)e.getValue());
                }
            }
        }
        return res.toArray(new DataFile[]{});
    }

    public DataFile[] getUndoFiles () {
        final List<DataFile> res = new ArrayList<DataFile>();
        for (Map.Entry e : ifs.entrySet()) {
            if (((DataFile)e.getValue()).getType()==UNDOFILE_TYPEID) {
                res.add((DataFile)e.getValue());
            }
        }
        return res.toArray(new DataFile[]{});
    }

    public DataFile[] getIndexFiles () {
        final List<DataFile> res = new ArrayList<DataFile>();
        for (Map.Entry e : ifs.entrySet()) {
            if (((DataFile)e.getValue()).getType()==INDXFILE_TYPEID) {
                res.add((DataFile)e.getValue());
            }
        }
        return res.toArray(new DataFile[]{});
    }

    public DataFile[] getTempFiles () {
        final List<DataFile> res = new ArrayList<DataFile>();
        for (Map.Entry e : ifs.entrySet()) {
            if (((DataFile)e.getValue()).getType()==TEMPFILE_TYPEID) {
                res.add((DataFile)e.getValue());
            }
        }
        return res.toArray(new DataFile[]{});
    }

    public void createSystemDataFiles (Session s, LLT llt) throws Exception {
        for (Map.Entry e : ifs.entrySet()) {
            if (((DataFile)e.getValue()).checkFile()==0) {
                ((DataFile)e.getValue()).createFile(s, llt);
            }
        }
    }

    public void openDataFiles () throws Exception {
        for (Map.Entry e : ifs.entrySet()) {
            final int state = ((DataFile)e.getValue()).checkFile();
            if (state == DataFile.FILE_OK) {
                ((DataFile)e.getValue()).openFile(Config.getConfig().DISKIO_MODE);
            }
            if (state == DataFile.FILE_VERSION_NOT_MATCH) {
                final int filesv = ((DataFile)e.getValue()).checkSystemVersion();
                logger.warn("\n----------------------------------------------------------------------\n" +
                        "--- data files were created by a different version of the software ---\n" +
                        "--- datafile version: "+filesv+" ---------------------------------------\n" +
                        "--- software version: "+Instance.SYSTEM_VERSION+" ---------------------------------------\n" +
                        "--- SERVICE MAY BE UNSTABLE ------------------------------------------\n" +
                        "----------------------------------------------------------------------");
                ((DataFile)e.getValue()).openFile(Config.getConfig().DISKIO_MODE);
            }
        }
    }

    public void closeDataFiles () throws IOException, InvalidFrame, ClassNotFoundException, InstantiationException, IllegalAccessException {
        for (Map.Entry e : ifs.entrySet()) {
            if (((DataFile)e.getValue()).isOpen()) {
                ((DataFile)e.getValue()).closeFile();
            }
        }
    }

    public DataFile getInitFile() throws ClassNotFoundException, InternalException, IllegalAccessException, InstantiationException {
        for (Map.Entry e : ifs.entrySet()) {
            if (((DataFile)e.getValue()).getFileId()==INITFILE_ID) {
                if (!((DataFile)e.getValue()).isOpen()) {
                    ((DataFile)e.getValue()).openFile(Config.getConfig().DISKIO_MODE);
                }
                return (DataFile)e.getValue();
            }
        }
        return null;
    }

    public int openStorage (DataFile[] files) throws ClassNotFoundException, InstantiationException, IllegalAccessException, InternalException {
        if (state==STORAGE_STATE_OPEN) {
            return state;
        }

        try {
            if (jrnFile == null) {
                jrnFile = new RandomAccessFile(Config.getConfig().JOURNAL_PATH + Instance.getInstance().getFileSeparator() + Instance.JRNL_FILE, Config.getConfig().DISKIO_MODE);
                jrnFile.seek(0);
            }
        } catch(Exception e) {
            logger.error(e.getMessage());
        }

        try {
            if (errFile == null) {
                errFile = new RandomAccessFile(Config.getConfig().JOURNAL_PATH + Instance.getInstance().getFileSeparator() + Instance.LOG_FILE, Config.getConfig().DISKIO_MODE);
                errFile.seek(errFile.length());
            }
        } catch(Exception e) {
            logger.error(e.getMessage());
        }

        if (files==null) {
            return STORAGE_STATE_EMPTY;
        }
        if (files.length==0) {
            return STORAGE_STATE_EMPTY;
        }

        int cnt = files.length;

        for (int i=0; i<files.length; i++) {
            RandomAccessFile f = files[i].openFile(Config.getConfig().DISKIO_MODE);
            if (f!=null) {
                cnt--;
            }
        }

//todo    if (cnt==0) {
        dfs = new HashMap<Integer, DataFile>();
        for (DataFile df : files) {
            dfs.put(df.getFileId(), df);
        }
        state = STORAGE_STATE_OPEN;
        return state;
//        } else {
//           return STORAGE_STATE_FAIL;
//        }

    }

    public void closeStorage (DataFile[] files) throws IOException, InvalidFrame {
        for (int i=0; i<files.length; i++) {
            files[i].closeFile();
        }
        try { if (!(jrnFile==null)) { jrnFile.close(); } } catch (IOException e) {}
        try { if (!(errFile==null)) { errFile.close(); } } catch (IOException e) {}
        jrnFile=null;
        errFile=null;
        dfs = null;
        state = STORAGE_STATE_CLOSED;
    }

    public void dropStorage () throws Exception {
        if (state==STORAGE_STATE_CLOSED) {
            for (Map.Entry e : ifs.entrySet()) {
                if (((DataFile)e.getValue()).checkFile()==1) {
                    File ff = new File(((DataFile)e.getValue()).getFileName());
                    ff.delete();
                }
            }
            File jf = new File(Config.getConfig().JOURNAL_PATH + Instance.getInstance().getFileSeparator() + Instance.JRNL_FILE);
            jf.delete();
            File ef = new File(Config.getConfig().JOURNAL_PATH + Instance.getInstance().getFileSeparator() + Instance.LOG_FILE);
            ef.delete();
        }
        state = STORAGE_STATE_EMPTY;
    }

    public int writeFrame (Frame b) throws IOException, InternalException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        final DataFile f = getDataFileById(b.getFile());
        f.writeFrame(b.getPointer(), b.getFrame());
        return 0;
    }

    public int writeFrame (byte[] b, long frameId) throws IOException, InternalException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        final int file = (int)frameId%4096;
        final long ptr = frameId - (frameId%4096);
        final DataFile f = getDataFileById(file);
        f.writeFrame(ptr, b);
        return 0;
    }

    public int writeFrameWithBackup (byte[] b, long frameId) throws IOException, InternalException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        final int file = (int)frameId%4096;
        final long ptr = frameId - (frameId%4096);
        final DataFile f = getDataFileById(file);
        final byte[] bb = f.readDataFromPtr(ptr, b.length);
        if (bb!=null) {
            jrnFile.seek(jrnptr.getAndAdd(bb.length));
            jrnFile.write(bb);
        }
        f.writeFrame(ptr, b);
        return 0;
    }

    public int writeFrameWithBackup (final DataFile f, final byte[] b, final long ptr) throws IOException, InternalException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        final byte[] bb = f.readDataFromPtr(ptr, b.length);
        if (bb != null) {
            jrnFile.seek(jrnptr.getAndAdd(bb.length));
            jrnFile.write(bb);
        }
        f.writeFrame(ptr, b);
        return 0;
    }

    //bootstrap subsystem - first index generate by objectId
    @SuppressWarnings("ConditionalBreakInInfiniteLoop")
    private synchronized IndexList bootstrapFrameLoad() throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, InternalException, NoSuchMethodException {
//        ArrayList<FrameData> res = new ArrayList<FrameData>();
        final IndexList res = new IndexList();
        //bulk Table (t) object for use in DataFrame->DataChunk parse methods
        final Table t = new Table(true);

        for (DataFile df : Storage.getStorage().getInitDataFiles()) {
            long start = Instance.getInstance().getFrameSize(); //first data frame
            while (true) {
                final FrameData bb = new FrameData(df.getFileId(), start, df.getType() == Storage.INDXFILE_TYPEID ? Instance.getInstance().getFrameSize2() : Instance.getInstance().getFrameSize(), t);
                DataFrame db = null;
                try {
                    db = bb.getDataFrame();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                start = db.getNextFrame();
//                start = bb.getNextFrame();
                for (Chunk c : db.getChunks()) {
                    if (c.getHeader().getState()==Header.RECORD_NORMAL_STATE) {  //miss deleted or archived records
                        final FrameData bd = (FrameData)((DataChunk)c).getEntity(FrameData.class, null);
                        if (bd.getFrameId()==bb.getFrameId()) {
                            bd.setFrame(db);
                        }
                        res.add(bd.getObjectId(), c);
                    }
                }
                if (start==0) { break; }
            }
        }

        return res;
    }

    private synchronized ArrayList<FrameData> getLBSArray(int objectId, IndexList ixl) {
        final List<Object> bds = ixl.getObjectsByKey(objectId);
        final ArrayList<FrameData> res = new ArrayList<FrameData>();
        for (Object b : bds) {
            final FrameData bd = (FrameData)((DataChunk)b).getEntity(FrameData.class, null);
            if (bd.getCurrent() > 0) {
                res.add(bd);
            }
        }
        return res;
    }

    public synchronized Table bootstrapLoad() throws Exception {
        Table res = null;
        final IndexList ixl = bootstrapFrameLoad();
        final List<Object> bds = ixl.getObjectsByKey(Table.CLASS_ID);
        final POJOProxyFactory ppf = POJOProxyFactory.getInstance();

        //Collections.sort(bds);
        final ConcurrentHashMap map1 = new ConcurrentHashMap();
        final ConcurrentHashMap map2 = new ConcurrentHashMap();

        for (Object b : bds) {
            final FrameData bd = (FrameData)((DataChunk)b).getEntity();
            bd.setEntityClass(Table.class);
            final DataFrame db = bd.getDataFrame();
            for (Chunk c : db.getChunks()) {
                if (c.getHeader().getState()==Header.RECORD_NORMAL_STATE) {  //miss deleted or archived records
                    //use bootstrap constructor
                    final Table td = (Table)((DataChunk)c).getEntity(Table.class, new Object[]{(DataChunk)c, ixl});
                    //reverse inject entity into datachunk
//                        ((DataChunk)c).setTable(td);
                    if (td.getName().equals("su.interference.persistent.Table")) {
                        //returns main Table object with Table indexes
                        res = td;
                    }

                    map1.put(td.getObjectId(), (DataChunk)c);
                    map2.put(td.getName(), (DataChunk)c);
                    final String tdname = td.getName();
                    final Class pc = ppf.register(tdname);
                    td.setSc(pc);
                    //if is not a SystemEntity (non-transactional)
                    if (pc!=null) {
                        map2.put(td.getSc().getName(), (DataChunk)c);
                    }
                }
            }
        }
        if (res!=null) {
            res.getMapFieldByColumn("objectId").setMap(map1);
            res.getMapFieldByColumn("name").setMap(map2);
        }
        return res;
    }


}
