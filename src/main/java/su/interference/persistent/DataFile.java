/**
 The MIT License (MIT)

 Copyright (c) 2010-2020 head systems, ltd

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
import su.interference.core.*;
import su.interference.exception.*;
import su.interference.metrics.Metrics;
import su.interference.mgmt.MgmtColumn;
import su.interference.serialize.ByteString;

import java.io.RandomAccessFile;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Modifier;
import java.lang.reflect.InvocationTargetException;
import javax.persistence.*;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

@Entity
@SystemEntity
public class DataFile implements Serializable {

    @Column
    @Id
    @GeneratedValue
    @IndexColumn
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int fileId;
    @Column
    @IndexColumn
    private int type;
    @Column
    @IndexColumn
    private int nodeId;
    @Column
    @MgmtColumn(width=70, show=true, form=true, edit=false)
    private String fileName;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private long fileSize;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private long fileUsed;
    @Column
    private int fileExtAmount;

    @Transient
    public static final int CLASS_ID = 4;
    @Transient
    public static final int SYSTEM_FRAME_SIZE = 4096;
    @Transient
    private final static long serialVersionUID = 8712349857239487287L;
    @Transient
    private final static Logger logger = LoggerFactory.getLogger(DataFile.class);
    @Transient
    protected RandomAccessFile file;
    @Transient
    protected SystemFrame sframe;
    @Transient
    public static final int FILE_NOT_FOUND = 0;
    @Transient
    public static final int FILE_OK = 1;
    @Transient
    public static final int FILE_VERSION_NOT_MATCH = 2;
    @Transient
    public static final int FILE_SYSTEM_FRAME_CORRUPT = 3;

    public static int getCLASS_ID() {
        return CLASS_ID;
    }

    public boolean isOpen() {
        return file != null;
    }

    public SystemFrame getSframe() {
        return sframe;
    }

    public RandomAccessFile getFile() {
        return file;
    }

    public void setFile(RandomAccessFile file) {
        this.file = file;
    }

    public DataFile() {
        
    }

    public boolean order(DataFile f) {
        final int order1 = f.getFileId()%Config.getConfig().FILES_AMOUNT==0?Config.getConfig().FILES_AMOUNT:f.getFileId()%Config.getConfig().FILES_AMOUNT;
        final int order2 = this.getFileId()%Config.getConfig().FILES_AMOUNT==0?Config.getConfig().FILES_AMOUNT:this.getFileId()%Config.getConfig().FILES_AMOUNT;
        return order1 == order2;
    }

    public boolean order(final int fileId) {
        final int order1 = fileId%Config.getConfig().FILES_AMOUNT==0?Config.getConfig().FILES_AMOUNT:fileId%Config.getConfig().FILES_AMOUNT;
        final int order2 = this.getFileId()%Config.getConfig().FILES_AMOUNT==0?Config.getConfig().FILES_AMOUNT:this.getFileId()%Config.getConfig().FILES_AMOUNT;
        return order1 == order2;
    }

    //constructor for low-level storage function (initial first-time load table descriptions from datafile)
    public DataFile (DataChunk chunk) throws IllegalAccessException, InternalException {
        final Object[] dcs = chunk.getDcs().getValueSet();
        final Class c = this.getClass();
        final java.lang.reflect.Field[] f = c.getDeclaredFields();
        int x = 0;
        for (int i=0; i<f.length; i++) {
            final Transient ta = f[i].getAnnotation(Transient.class);
            if (ta==null) {
                final int m = f[i].getModifiers();
                if (Modifier.isPrivate(m)) {
                    f[i].setAccessible(true);
                }
                f[i].set(this, dcs[x]);
                x++;
            }
        }
    }

    public DataFile(int fileId, int type, String fileName) {
        this.fileId   = fileId;
        this.type     = type;
        this.fileName = fileName;
    }

    //todo need optimization (redundancy with store frame to disk on change prev-next values of FrameData)
    //causes deadlock by reorder of access:
    //normal order of access - lock datafile, then lock table<framedata>
    //but, in case of allocate undo space this may looks as:
    //lock datafile<undo> - lock table<framedata> - try lock datafile
    //todo deprecated started param
    public FrameData createNewFrame(FrameData frame, WaitFrame wb, int frameType, long allocId, boolean started, boolean external, Table t, Session s, LLT llt) throws Exception {
        //deadlock bug fix
        //instead this.allocateFrame we lock Table<FrameData> first
        final FrameData bd = t.allocateFrame(this, t, s, llt);
        bd.setFrameType(frameType);
        synchronized (this) {
            final boolean setcurrenable = external ? false : t.getName().equals("su.interference.persistent.UndoChunk") ? false : true;
            //allocated for rframe
            if (allocId > 0) {
                bd.setAllocId(allocId);
            }

            //todo deprecated
            if (started) {
                bd.setStarted(1);
            }

            final Frame db = frameType == 0 ? new DataFrame(bd, t) : new IndexFrame(bd, frameType, t);
            db.setObjectId(t.getObjectId());
            bd.setFrame(db);
            if (setcurrenable) bd.markAsCurrent();
            int prevFile = 0;
            long prevPtr = 0;

            if (frame != null && frameType == 0) {
                frame.clearCurrent();
                prevFile = frame.getFile();
                prevPtr = frame.getPtr();
                frame.setNextFile(bd.getFile());
                frame.setNextFrame(bd.getPtr());
                frame.getDataFrame().setNextFile(bd.getFile());
                frame.getDataFrame().setNextFrame(bd.getPtr());
                s.persist(frame, llt); //update
            }

            bd.setPrevFile(prevFile);
            bd.setPrevFrame(prevPtr);

            if (frameType == 0) {
                ((DataFrame) db).setPrevFile(prevFile);
                ((DataFrame) db).setPrevFrame(prevPtr);
            }

            if (t.getFileStart() == 0 && t.getFrameStart() == 0) {
                t.setFileStart(bd.getFile());
                t.setFrameStart(bd.getPtr());
            }

            t.setFileLast(bd.getFile());
            t.setFrameLast(bd.getPtr());
            t.incFrameAmount();

            if (!external) {
                if (llt != null) {
                    llt.add(db);
                    if (frame != null) {
                        llt.add(frame.getFrame());
                    }
                }
            }

            s.persist(t, llt); //update

            if (t.getName().equals("su.interference.persistent.FrameData")) {
                DataChunk dc = new DataChunk(bd, s);
                int len = dc.getBytesAmount();
                t.usedSpace(bd, len, false, s, llt);
                //replace chunk after usedSpace
                dc = new DataChunk(bd, s);
                db.insertChunk(dc, s, true, llt);
                t.addIndexValue(dc);
            }
            if (wb != null) {
                wb.set(bd);
            }
        }
        if (!t.getName().equals("su.interference.persistent.FrameData")) {
            //syncframe event should not persist new frame
            if (!external) {
                //fix deadlock by reorder access
                //if current object is not DATAFILE, then locks DATAFILE first
                if (this.getFileId() == bd.getFile()) {
                    s.persist(bd, llt);
                } else {
                    DataFile df = Instance.getInstance().getDataFileById(bd.getFile());
                    df.persist(bd, s, llt);
                }
            }
        }
        return bd;
    }

    private synchronized void persist(Object o, Session s, LLT llt) throws Exception {
        final Table t = Instance.getInstance().getTableByName(o.getClass().getName());
        if (t!=null) {
            t.persist(o, s, llt);
        }
    }

    // 0 - file not found
    // 1 - file OK
    // 2 - System versions don't match
    // 3 - SystemFrame corrupt or parameters don't match
    public synchronized int checkFile() throws Exception {
        try {
            this.file = new RandomAccessFile(this.fileName,"r");
            byte[] b = new byte[SYSTEM_FRAME_SIZE];
            try {
                this.file.seek(0);
                this.file.read(b,0,SYSTEM_FRAME_SIZE);
                this.sframe = new SystemFrame(b,this.getFileId(),0);
                try {
                    this.file.close();
                } catch (IOException e) {
                    return FILE_SYSTEM_FRAME_CORRUPT;
                }
                if (this.sframe.getSysVersion()==Instance.SYSTEM_VERSION) {
                    return FILE_OK;
                } else {
                    return FILE_VERSION_NOT_MATCH;
                }
            } catch (FileNotFoundException e) {
                return FILE_NOT_FOUND;
            } catch (IOException e) {
                return FILE_SYSTEM_FRAME_CORRUPT;
            } catch (EmptyFrameHeaderFound e) {
                return FILE_SYSTEM_FRAME_CORRUPT;
            } catch (InvalidFrameHeader e) {
                return FILE_SYSTEM_FRAME_CORRUPT;
            } catch (InvalidFrame e) {
                return FILE_SYSTEM_FRAME_CORRUPT;
            }
        } catch (FileNotFoundException e) {
            return FILE_NOT_FOUND;
        }
    }

    public synchronized int checkSystemVersion() throws Exception {
        try {
            this.file = new RandomAccessFile(this.fileName,"r");
            byte[] b = new byte[SYSTEM_FRAME_SIZE];
            try {
                this.file.seek(0);
                this.file.read(b,0,SYSTEM_FRAME_SIZE);
                this.sframe = new SystemFrame(b,this.getFileId(),0);
                try {
                    this.file.close();
                } catch (IOException e) {
                    return 0;
                }
                return this.sframe.getSysVersion();
            } catch (Exception e) {
                return 0;
            }
        } catch (FileNotFoundException e) {
            return 0;
        }

    }

    public synchronized RandomAccessFile openFile(String mode) {
        boolean exist = false;
        try {
            this.file = new RandomAccessFile(this.fileName,"r");
            exist = true;
            try {
                this.file.close();
            } catch (IOException e) {
                logger.info("openFile throws IOException during close "+ this.fileName);
            }
        } catch (FileNotFoundException e) {
            logger.info("openFile throws FileNotFoundException during open "+ this.fileName);
        }
        if (exist) {
            try {
                this.file = new RandomAccessFile(this.fileName,mode);
            } catch (FileNotFoundException e) {
                logger.info("openFile throws FileNotFoundException during open "+ this.fileName);
            }

            byte[] b = new byte[SYSTEM_FRAME_SIZE];
            if (!(this.file==null)) {
                try {
                    this.file.seek(0);
                    this.file.read(b,0,SYSTEM_FRAME_SIZE);
                    this.sframe = new SystemFrame(b,this.getFileId(),0);
                    final SystemData sd = this.sframe.getSystemData();
                    logger.info("open " + this.fileName + " file completed successfully (ver." + sd.getVersion() + ")");
                } catch (Exception e) {
                    logger.error("open " + this.fileName + " file throws " + e.getMessage());
                }
            }
        }
        return this.file;
    }
    
    public synchronized void closeFile() throws IOException, InvalidFrame {
        //sync header
        if (this.sframe!=null) {
            writeFrame(0, this.sframe.getFrame());
        }
        if (this.file!=null) {
            this.file.close();
            this.file = null;
        }
    }

    public synchronized void createFile(Session s, LLT llt) throws IOException, InvocationTargetException, NoSuchMethodException, FileNotFoundException, InternalException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        boolean check;
        try {
            this.file = new RandomAccessFile(this.fileName,"r");
            check = true;
            this.file.close();
        } catch (FileNotFoundException e) {
            check = false;
        } catch (IOException e) {
            check = true;
        }

        if (check) {
            throw new InternalException();
        } else {
            this.file = new RandomAccessFile(this.fileName,"rws");
            createHeaderFrame(s, llt);
            this.file.close();
        }
    }

    private synchronized void createHeaderFrame(Session s, LLT llt) throws IOException, InvocationTargetException, NoSuchMethodException, InternalException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        this.sframe = new SystemFrame(this.getFileId(), 0, type == Storage.INDXFILE_TYPEID ? Instance.getInstance().getFrameSize2() : Instance.getInstance().getFrameSize());
        this.sframe.insertSU(s, llt);
//        Storage.getStorage().writeFrame(this.sframe);
        this.writeFrame(0, this.sframe.getFrame());
    }

    public synchronized FrameData allocateFrame(Table t, Session s, LLT llt) throws Exception {
        final int size = t.getFrameSize();
        Metrics.get("reallocateFrame").start();
        if (Instance.getInstance().getSystemState()==Instance.SYSTEM_STATE_UP) {
            final FreeFrame fframe = getReallocFreeFrame(size);
            if (fframe != null) {
                final FrameData bd = new FrameData(fframe.getFile(), fframe.getPtr(), fframe.getSize(), t);
                // t should be updated mandatory during createFrame
                bd.setFrameOrder(t.getFrameOrder(s, llt));
                s.delete(fframe, llt);
                Metrics.get("reallocateFrame").stop();
                return bd;
            }
        }
        Metrics.get("reallocateFrame").stop();
        Metrics.get("allocateFrame").start();
        final int fileExt = this.sframe.getExtSize();
        final long ptr = this.sframe.getLastFramePtr();
        final long fileAmt = this.file.length();
        final long newAmt = ptr + size;
        if (fileAmt < newAmt) {
            extendFile(fileExt);
        }
        this.sframe.setLastFramePtr(newAmt);
//        Storage.getStorage().writeFrame(this.sframe);
        this.writeFrame(0, this.sframe.getFrame());
        final FrameData bd = new FrameData(this.getFileId(), ptr, size, t);
        // t should be updated mandatory during createFrame
        bd.setFrameOrder(t.getFrameOrder(s, llt));
        Metrics.get("allocateFrame").stop();
        return bd;
    }

    private synchronized FreeFrame getReallocFreeFrame(int size) {
        final Table t = Instance.getInstance().getTableByName("su.interference.persistent.FreeFrame");
        final Object o = t.getIndexFieldByColumn("fileId").getIndex().getFirstObjectByKey(this.fileId);
        if (o != null) {
            final FreeFrame fframe = (FreeFrame) ((DataChunk) o).getEntity();
            if (fframe.getSize() == size && fframe.getFile() == this.fileId && fframe.getPassed() == 1) {
//            if (fframe.getSize() == size && fframe.getFile() == this.fileId) {
                return (FreeFrame) ((DataChunk) o).getEntity();
            }
        }
        return null;
    }

    public synchronized void extendFile(long size) throws IOException {
        this.file.setLength(this.file.length()+size);
    }

    //todo main method must incapsulate all cache-depends functional
    public synchronized byte[] readData(final long ptr, final int size) throws IOException, InternalException {
        final long rest = this.file.length() - ptr;
        if (rest < size) {
            throw new InternalException();
        }
        final byte[] b = new byte[size];
        this.file.seek(ptr);
        this.file.read(b, 0, size);
        return b;
    }

    //returns all bytes from ptr until end of file or size
    public byte[] readDataFromPtr(long ptr, int size) throws IOException {
        long rest = this.file.length() - ptr;
        if (rest<=0) {
            return null;
        }
        if (rest > size) {
            rest = size;
        }
        byte[] b = new byte[(int)rest];
        this.file.seek(ptr);
        this.file.read(b, 0, (int)rest);
        return b;
    }

    public synchronized void writeFrame(final long ptr, final byte[] b) throws IOException {
        final ByteString bs = new ByteString(b);
        final int file_ = bs.getIntFromBytes(0);
        final long ptr_ = bs.getLongFromBytes(4);
        final int id_ = bs.getIntFromBytes(12);

        if (file_ == 0) {
            logger.error("Wrong write frame operation with file = 0 ptr = " + ptr_);
        }

        if (this.fileId != file_) {
            logger.error("Wrong write frame operation with file = " + this.file + ", internal file = " + file_ + " ptr = " + ptr_);
        }

        if (ptr != ptr_) {
            logger.error("Wrong write frame operation with file = " + this.file + " ptr = " + ptr + ", internal file = " + file_ + " ptr = " + ptr_);
        }

        this.file.seek(ptr);
        this.file.write(b);
    }

    public void writeFrame(FrameData bd, final long ptr, final byte[] b, LLT llt, Session s) throws Exception {
        final ByteString bs = new ByteString(b);
        final int file_ = bs.getIntFromBytes(0);
        final long ptr_ = bs.getLongFromBytes(4);
        final int id_ = bs.getIntFromBytes(12);

        if (this.fileId != file_) {
            logger.error("Wrong write frame operation with file = " + this.file + ", internal file = " + file_ + " ptr = " + ptr_);
        }

        if (ptr != ptr_) {
            logger.error("Wrong write frame operation with file = " + this.file + " ptr = " + ptr + ", internal file = " + file_ + " ptr = " + ptr_);
        }
        synchronized (this) {
            this.file.seek(ptr);
            this.file.write(b);
        }
        s.persist(bd, llt);
    }

    public boolean isData() {
        return this.type == Storage.DATAFILE_TYPEID;
    }

    public boolean isIndex() {
        return this.type == Storage.INDXFILE_TYPEID;
    }

    public boolean isUndo() {
        return this.type == Storage.UNDOFILE_TYPEID;
    }

    public boolean isTemp() {
        return this.type == Storage.TEMPFILE_TYPEID;
    }

    public int getFileId() {
        return fileId;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public long getFileUsed() {
        return fileUsed;
    }

    public void setFileUsed(long fileUsed) {
        this.fileUsed = fileUsed;
    }

    public int getFileExtAmount() {
        return fileExtAmount;
    }

    public void setFileExtAmount(int fileExtAmount) {
        this.fileExtAmount = fileExtAmount;
    }

}
