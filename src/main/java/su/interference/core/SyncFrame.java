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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SyncFrame implements Comparable, Serializable, AllowRPredicate {

    private final static Logger logger = LoggerFactory.getLogger(SyncFrame.class);
    private final byte[] b;
    private final long frameId;
    private final long allocId;
    private final int fileType;
    private final int frameType;
    private final String className;
    private long prevId;
    private long nextId;
    private final long parentId;
    private final long lcId;
    private final boolean allowR;
    private final boolean started;
    private FrameData bd;
    private DataFile df;
    private Frame rFrame;
    private final HashMap<Long, Long> imap;
    private final HashMap<Long, Transaction> rtran;
    private final ArrayList<TransFrame> tframes;
    private final static long serialVersionUID = 8712349857239487289L;

    public SyncFrame(Frame frame, Session s, FreeFrame fb) throws Exception {
        final Table t = Instance.getInstance().getTableById(frame.getObjectId());
        final FrameData bd = Instance.getInstance().getFrameById(frame.getPtr());
        allowR = frame.isLocal() ? !t.isNoTran() || t.getName().equals("su.interference.persistent.UndoChunk") : false;

        if (bd == null && allowR) {
            final FreeFrame fframe = Instance.getInstance().getFreeFrameById(frame.getPtr());
            if (fframe == null) {
                logger.error(frame.getClass().getSimpleName()+" does not match any system objects");
                throw new InternalException();
            } else {
                fframe.setPassed(1);
                fb = fframe;
            }
            //throw new MissingSyncFrameException();
        }

        className = bd == null ? null : t.getName();
        rtran = frame.getLiveTransactions();
        tframes = frame.getLiveTransFrames();
        if (frame.getClass().getName().equals("su.interference.core.DataFrame")) {
            if (frame.getType()!=0) {
                throw new InternalException();
            }
            started = false;
            final DataFrame db = (DataFrame) frame;
            imap = db.getAllocateMap();
            final long prevId_ = db.getPrevFrame()+db.getPrevFile();
            final long nextId_ = db.getNextFrame()+db.getNextFile();
            try {
                //todo NPE possibly by evicted frame
                prevId = prevId_ == 0 ? 0 : Instance.getInstance().getFrameById(prevId_).getAllocId();
                nextId = nextId_ == 0 ? 0 : Instance.getInstance().getFrameById(nextId_).getAllocId();
            } catch (Exception e) {
                e.printStackTrace();
            }
            parentId = 0;
            lcId = 0;
            this.frameType = t.getName().equals("su.interference.persistent.UndoChunk") ? 99 : frame.getType();
        } else if (frame.getClass().getName().equals("su.interference.core.IndexFrame")) {
            if (frame.getType()==0|| frame.getType()>2) {
                throw new InternalException();
            }
            final int fileId = (int) frame.getPtr()%4096;
            final long ptr = frame.getPtr() - frame.getPtr()%4096;
            if (t.getFileStart()==fileId&&t.getFrameStart()==ptr) {
                started = true;
            } else {
                started = false;
            }
            final IndexFrame ib = (IndexFrame) frame;
            imap = ib.getAllocateMap();
            prevId = 0;
            nextId = 0;
            final long parentId_ = ib.getParentF()+ib.getParentB();
            final long lcId_ = ib.getLcF()+ib.getLcB();
            parentId = parentId_==0?0:Instance.getInstance().getFrameById(parentId_).getAllocId();
            lcId = lcId_==0?0:Instance.getInstance().getFrameById(lcId_).getAllocId();
            this.frameType = frame.getType();
        } else {
            throw new InternalException();
        }
        fileType = Instance.getInstance().getDataFileById(frame.getFile()).getType();
        b = frame.getFrame();
        frameId = frame.getPtr();
        this.allocId = frame.getAllocFile()+ frame.getAllocPointer();
    }

    public Frame getRFrame() {
        return rFrame;
    }

    public void setRFrame(Frame rFrame) {
        this.rFrame = rFrame;
    }

    public byte[] getBytes() {
        return b;
    }

    public long getFrameId() {
        return frameId;
    }

    public long getAllocId() {
        return allocId;
    }

    public int getFileType() {
        return fileType;
    }

    public int getFrameType() {
        return frameType;
    }

    public int getFile() {
        return (int)frameId%4096;
    }

    public long getPointer() {
        return frameId - (frameId%4096);
    }

    public String getClassName() {
        return className;
    }

    public long getPrevId() {
        return prevId;
    }

    public long getNextId() {
        return nextId;
    }

    public long getParentId() {
        return parentId;
    }

    public long getLcId() {
        return lcId;
    }

    public boolean isAllowR() {
        return allowR;
    }

    public boolean isStarted() {
        return started;
    }

    public FrameData getBd() {
        return bd;
    }

    public void setBd(FrameData bd) {
        this.bd = bd;
    }

    public DataFile getDf() {
        return df;
    }

    public void setDf(DataFile df) {
        this.df = df;
    }

    public HashMap<Long, Long> getImap() {
        return imap;
    }

    public HashMap<Long, Transaction> getRtran() {
        return rtran;
    }

    public ArrayList<TransFrame> getTframes() {
        return tframes;
    }

    public boolean equals (SyncFrame bl) {
        if ((this.getFile()==bl.getFile())&&(this.getPointer()==bl.getPointer())) {
            return true;
        } else {
            return false;
        }
    }

    public int compareTo(Object obj) {
        SyncFrame b = (SyncFrame)obj;
        if (this.getPointer()<b.getPointer()) {
            return -1;
        }
        if (this.getPointer()==b.getPointer()) {
            return 0;
        }
        if (this.getPointer()>b.getPointer()) {
            return 1;
        }
        return 0;
    }

}
