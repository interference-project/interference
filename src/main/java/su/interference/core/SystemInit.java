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
import su.interference.exception.InternalException;
import su.interference.persistent.*;
import su.interference.persistent.Process;

import javax.persistence.Column;
import javax.persistence.Transient;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SystemInit {
    private final static int INITIAL_CLASSES_AMT = 15;
    private final static Logger logger = LoggerFactory.getLogger(SystemInit.class);

    public static Table initSystem (boolean initStorage, int nodeType, Session s) throws Exception {
        return createInitialFrames (initStorage, nodeType, s);
    }

    //list of system non-transactional classes
    public static String[] getInitialClasses() {
        final String[] initialClasses = new String[INITIAL_CLASSES_AMT];
        initialClasses[0] = "su.interference.persistent.FrameData";
        initialClasses[1] = "su.interference.persistent.Table";
        initialClasses[2] = "su.interference.persistent.Field";
        initialClasses[3] = "su.interference.persistent.DataFile";
        initialClasses[4] = "su.interference.persistent.FreeFrame";
        initialClasses[5] = "su.interference.persistent.Session";
        initialClasses[6] = "su.interference.persistent.Transaction";
        initialClasses[7] = "su.interference.persistent.TransFrame";
        initialClasses[8] = "su.interference.persistent.UndoChunk";
        initialClasses[9] = "su.interference.persistent.Process";
        initialClasses[10] = "su.interference.persistent.RetrieveLock";
        initialClasses[11] = "su.interference.persistent.Node";
        initialClasses[12] = "su.interference.persistent.MgmtModule";
        initialClasses[13] = "su.interference.persistent.Cursor";
        initialClasses[14] = "su.interference.persistent.FrameSync";
        return initialClasses;
    }

   private static String[] getFileDeps() {
        final String[] deps = new String[5];
        deps[0] = "su.interference.persistent.FrameData";
        deps[1] = "su.interference.persistent.DataFile";
        deps[2] = "su.interference.persistent.FreeFrame";
        deps[3] = "su.interference.persistent.TransFrame";
        deps[4] = "su.interference.persistent.UndoChunk";
        return deps;
    }

    private static boolean isFileDep(String cls) {
        for (String c : getFileDeps()) {
            if (c.equals(cls)) {
                return true;
            }
        }
        return false;
    }

    private static Table getTableById(Table[] ts, int id) {
        for (Table t : ts) {
            if (t.getObjectId()==id) {
                return t;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public static Table createInitialFrames(boolean initStorage, int nodeType, Session s) throws Exception {

        ClassLoader cl = Instance.getInstance().getClass().getClassLoader();
        Table tt = null;

        String[] tables = getInitialClasses();
        Table[] ts = new Table[tables.length];
        DataFile[] dfs = Storage.getStorage().getInitFilesAsArray();
        DataFile[] ufs = Storage.getStorage().getUndoFiles();

        int framesAmount = 0;
        int framesCntr = 0;
        int tframeCntr = 0;
        for (String c : tables) {
            framesAmount = framesAmount + (isFileDep(c)?Config.getConfig().FILES_AMOUNT:1);
        }

        FrameData[] bds = new FrameData[framesAmount];
        //FrameData[] uds = new FrameData[tables.length];
        Frame[] ret  = new Frame[framesAmount];
        //UndoFrame[] undo = new UndoFrame[tables.length];
        ConcurrentHashMap map1 = new ConcurrentHashMap();
        ConcurrentHashMap map2 = new ConcurrentHashMap();

        for (int i=0; i<tables.length; i++) {
            Class cls = null;
            try {
                cls = cl.loadClass(tables[i]);
            } catch(ClassNotFoundException e) {
                logger.info("ClassNotFoundException during createInitialFrames for "+tables[i]);
            }

            int classId = 0;

            try {
                java.lang.reflect.Field idf = cls.getField("CLASS_ID");
                classId = idf.getInt(null);
            } catch (NoSuchFieldException e) {
                classId = i+100;
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }

            Table t = new Table(classId, tables[i]); //objectId
            if (s!=null) {
                t.setFrameSize(Instance.getInstance().getFrameSize());
            }

            if (tables[i].equals("su.interference.persistent.UndoChunk")) {
                t.setFileStart(Storage.getStorage().UNDOFILE_ID);
                t.setFileLast(Storage.getStorage().UNDOFILE_ID);
            } else {
                t.setFileStart(Storage.getStorage().INITFILE_ID);
                t.setFileLast(Storage.getStorage().INITFILE_ID);
            }

            if (tables[i].equals("su.interference.persistent.Table")) {
                tt = t;
                t.setIdValue(new AtomicLong(999));
            } else
            if (tables[i].equals("su.interference.persistent.Node")) {
                t.setIdValue(new AtomicLong(1));
            } else
            if (tables[i].equals("su.interference.persistent.DataFile")) {
                t.setIdValue(new AtomicLong(255));
            } else
            if (tables[i].equals("su.interference.persistent.Transaction")) {
                t.setIdValue(new AtomicLong(Config.getConfig().LOCAL_NODE_ID));
                t.setIdIncrement(Storage.MAX_NODES);
            } else {
                t.setIdValue(new AtomicLong(0));
            }

            if (initStorage) {
                DataFile df = Storage.getStorage().getInitFile();
                if (isFileDep(tables[i])) {
                    int ufsCntr = 0;
                    for (int x=1; x<dfs.length; x++) {
                        DataFile f = dfs[x];
                        if (f.getType() == Storage.DATAFILE_TYPEID) {
                            FrameData frame = f.allocateFrame(t, s, null);
                            frame.markAsCurrent();
                            if (f.getFileId() == df.getFileId()) {
                                t.setFrameStart(frame.getPtr());
                                t.setFrameLast(frame.getPtr());
                            }
                            bds[framesCntr] = frame;
                            //create dataframe
                            if (tables[i].equals("su.interference.persistent.UndoChunk")) {
                                //dirty hack create bulk unused frame in datafile
                                ret[framesCntr] = new DataFrame(ufs[ufsCntr].getFileId(), bds[framesCntr].getPtr(), t.getFrameSize(), t);
                                ufsCntr++;
                            } else {
                                ret[framesCntr] = new DataFrame(bds[framesCntr].getFile(), bds[framesCntr].getPtr(), t.getFrameSize(), t);
                            }
                            ret[framesCntr].setObjectId(t.getObjectId());
                            bds[framesCntr].setFrame(ret[framesCntr]);
                            if (tables[i].equals("su.interference.persistent.DataFile")) {
                                DataChunk dc = new DataChunk(f, s);
                                ret[framesCntr].insertChunk(dc, s, true, null);
                                for (int y=1; y<dfs.length; y++) {
                                    if (dfs[y].getType() != Storage.DATAFILE_TYPEID) {
                                        int p = Config.getConfig().FILES_AMOUNT*(dfs[y].getType()-1);
                                        if (f.getFileId()==(dfs[y].getFileId()-p)) {
                                            dc = new DataChunk(dfs[y], s);
                                            ret[framesCntr].insertChunk(dc, s, true, null);
                                        }
                                    }
                                }
                            }
                            framesCntr++;
                        }
                    }
                } else {
                    FrameData iniFrame = df.allocateFrame(t, s, null);
                    iniFrame.markAsCurrent();
                    t.setFrameStart(iniFrame.getPtr());
                    t.setFrameLast(iniFrame.getPtr());
                    bds[framesCntr] = iniFrame;
                    //create dataframe
                    if (tables[i].equals("su.interference.persistent.UndoChunk")) {
                        //dirty hack create bulk unused frame in datafile
                        ret[framesCntr] = new DataFrame(Storage.getStorage().UNDOFILE_ID, t.getFrameStart(), t.getFrameSize(), t);
                    } else {
                        ret[framesCntr] = new DataFrame(t.getFileStart(), t.getFrameStart(), t.getFrameSize(), t);
                    }
                    ret[framesCntr].setObjectId(t.getObjectId());
                    bds[framesCntr].setFrame(ret[framesCntr]);
                    //undo[i].setObjectId(t.getObjectId());
                    if (tables[i].equals("su.interference.persistent.Table")) {
                        tframeCntr = framesCntr;
                    }
                    if (tables[i].equals("su.interference.persistent.Process")) {
                        //insert process
                        ret[framesCntr].insertChunk(new DataChunk(new Process(1, "hbeat","su.interference.transport.HeartBeatProcess"), s), s, true, null);
                        ret[framesCntr].insertChunk(new DataChunk(new Process(2, "lsync","su.interference.core.SyncQueue"), s), s, true, null);
                        ret[framesCntr].insertChunk(new DataChunk(new Process(3, "clean","su.interference.core.SystemCleanUp"), s), s, true, null);
                        ret[framesCntr].insertChunk(new DataChunk(new Process(4, "trcln","su.interference.core.TransCleanUp"), s), s, true, null);
                        //ret[framesCntr].insertChunk(new DataChunk(new Process(2, "rsync","su.interference.remote.RemoteSync"), s), s, true, null);
                    }
                    if (tables[i].equals("su.interference.persistent.Node")) {
                        //insert node
                        Node n = new Node(Instance.getInstance().getLocalNodeId());
                        n.setType(nodeType);
                        ret[framesCntr].insertChunk(new DataChunk(n, s), s, true, null);
                    }
                    if (tables[i].equals("su.interference.persistent.MgmtModule")) {
                        //insert modules
                        ret[framesCntr].insertChunk(new DataChunk(new MgmtModule(1,"Instance","su.interference.mgmt.InstanceMgmt",1),s),s,true, null);
                        ret[framesCntr].insertChunk(new DataChunk(new MgmtModule(2,"Cluster","su.interference.persistent.Node",1),s),s,true, null);
                        ret[framesCntr].insertChunk(new DataChunk(new MgmtModule(3,"Datafiles","su.interference.persistent.DataFile",1),s),s,true, null);
                        ret[framesCntr].insertChunk(new DataChunk(new MgmtModule(4,"Tables","su.interference.persistent.Table",1),s),s,true, null);
                        ret[framesCntr].insertChunk(new DataChunk(new MgmtModule(5,"Frames","su.interference.persistent.FrameData",1),s),s,true, null);
                        ret[framesCntr].insertChunk(new DataChunk(new MgmtModule(6,"Processes","su.interference.persistent.Process",1),s),s,true, null);
                        ret[framesCntr].insertChunk(new DataChunk(new MgmtModule(7,"Session","su.interference.persistent.Session",1),s),s,true, null);
                        ret[framesCntr].insertChunk(new DataChunk(new MgmtModule(8,"Transactions","su.interference.persistent.Transaction",1),s),s,true, null);
                        ret[framesCntr].insertChunk(new DataChunk(new MgmtModule(9,"Sources","su.interference.persistent.Source",1),s),s,true, null);
                        ret[framesCntr].insertChunk(new DataChunk(new MgmtModule(10,"Modules","su.interference.persistent.MgmtModule",1),s),s,true, null);
                    }
                    framesCntr++;
                }

                t.setFrameAmount(isFileDep(tables[i])?Config.getConfig().FILES_AMOUNT:1);
            }
            ts[i] = t;
        }

        //insert tables
        for (int i=0; i<ts.length; i++) {
            DataChunk dc = new DataChunk(ts[i], s);
            if (initStorage) {
                ret[tframeCntr].insertChunk(dc, s, true, null);
            }
            map1.put(ts[i].getObjectId(), dc);
            map2.put(ts[i].getName(), dc);
        }

        //set indexes into tt
        tt.getMapFieldByColumn("objectId").setMap(map1);
        tt.getMapFieldByColumn("name").setMap(map2);

        //insert downtime indexes into tables
        ts[11].addIndexValue(new DataChunk(new Node(Instance.getInstance().getLocalNodeId()),s));
        ts[12].addIndexValue(new DataChunk(new MgmtModule(1,"Instance","su.interference.mgmt.InstanceMgmt",1),s));

        if (initStorage) {
            for (Frame b : ret) {
                Storage.getStorage().writeFrame(b);
            }

            for (int i=0; i<bds.length; i++) {
                if (getTableById(ts, bds[i].getObjectId()).getName().equals("su.interference.persistent.FrameData")) {
                    for (int j=0; j<bds.length; j++) {
                        if (!getTableById(ts, bds[j].getObjectId()).getName().equals("su.interference.persistent.UndoChunk")) {
                            if (isFileDep(getTableById(ts, bds[i].getObjectId()).getName())) {
                                if (bds[i].getFile() == bds[j].getFile()) {
                                    DataChunk dc = new DataChunk(bds[j], s);
                                    bds[i].getFrame().insertChunk(dc, s, true, null);
                                }
                            } else {
                                if (bds[i].getFile() == Storage.getStorage().INITFILE_ID) {
                                    DataChunk dc = new DataChunk(bds[j], s);
                                    bds[i].getFrame().insertChunk(dc, s, true, null);
                                }
                            }
                        }
                    }
                }
            }

            for (int i=0; i<bds.length; i++) {
                bds[i].setUsed(ret[i].getBytesAmountWoHead());

                ArrayList<Object> list = bds[i].getFrameEntities(s);
                Class c = list.size()==0?null:list.get(0).getClass();
                if (c!=null) {
                    java.lang.reflect.Field[] fs = c.getDeclaredFields();
                    for (Object o : list) {
                        String str = "";
                        for (int z=0; z<fs.length; z++) {
                            java.lang.reflect.Field f = fs[z];
                            Column a = f.getAnnotation(Column.class);
                            Transient ta = f.getAnnotation(Transient.class);
                            if (ta==null) {
                                Method mt = c.getMethod("get"+f.getName().substring(0,1).toUpperCase()+f.getName().substring(1,f.getName().length()), null);
                                str = str + f.getName()+": "+getStringValue(mt.invoke(o, null)) + " ";
                            }
                        }
                        System.out.println(str);
                    }
                }

            }

            for (Frame b : ret) {
                Storage.getStorage().writeFrame(b);
            }
        }
        return tt;
    }

    public static String getStringValue (Object o) throws ClassNotFoundException, InternalException, IllegalAccessException, InstantiationException {
        if (o==null) return "null";
        String t = o.getClass().getName();
        if (!Types.isPrimitiveType(t)) {
            if (o==null) {
                return "";
            }
            if (t.equals("java.lang.String")) {
                return (String)o;
            }
            if (t.equals("java.lang.Integer")) {
                return String.valueOf(o);
            }
            if (t.equals("java.lang.Long")) {
                return String.valueOf(o);
            }
            if (t.equals("java.util.concurrent.atomic.AtomicInteger")) {
                return String.valueOf(o);
            }
            if (t.equals("java.util.concurrent.atomic.AtomicLong")) {
                return String.valueOf(o);
            }
            if (t.equals("java.lang.Float")) {
                return String.valueOf(o);
            }
            if (t.equals("java.lang.Double")) {
                return String.valueOf(o);
            }
            if (t.equals("java.util.Date")) {
                SimpleDateFormat df = new SimpleDateFormat(Instance.getInstance().getDateFormat());
                return df.format(o);
            }
            return String.valueOf(o);
        } else {
            return String.valueOf(o);
        }
    }


}
