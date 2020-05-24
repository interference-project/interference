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

import su.interference.exception.InvalidFrameHeader;
import su.interference.exception.EmptyFrameHeaderFound;
import su.interference.exception.InvalidFrame;
import su.interference.exception.InternalException;
import su.interference.persistent.Table;
import su.interference.persistent.Session;
import su.interference.serialize.ByteString;

import java.io.IOException;
import java.util.Date;
import java.lang.reflect.InvocationTargetException;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SystemFrame extends Frame {

    public SystemFrame(int file, long pointer, int size) throws InternalException {
        super (file,pointer,size,null);
        this.setLastFramePtr(size);
        this.setSystemDBL(1398362964);
        this.setSystemDBR(1162691650);
        this.setExtSize(65536);
        this.setSysVersion(Instance.SYSTEM_VERSION);
        this.setTblStartFrame(size);
        this.setColStartFrame(size*2);
    }

    public SystemFrame(byte[] b, int file, long pointer) throws IOException, NoSuchMethodException, ClassNotFoundException, InstantiationException, IllegalAccessException, InternalException {
        super (b, file, pointer, 0, null, null, null) ;
        int ptr = FRAME_HEADER_SIZE;
        final ByteString bs = new ByteString(this.b);
        while (ptr<b.length) {
            if (b.length>=ptr+ROW_HEADER_SIZE) {
                RowHeader h = new RowHeader(bs.substring(ptr, ptr+ROW_HEADER_SIZE), this.getFile(), this.getPointer());
                if ((h.getPtr()>0)&&(h.getLen()>0)) {
                    data.add(new DataChunk(bs.substring(ptr, ptr+ROW_HEADER_SIZE+h.getLen()), this.getFile(), this.getPointer(), ROW_HEADER_SIZE, new Table("su.interference.core.SystemData"), SystemData.class));
                    ptr = ptr + ROW_HEADER_SIZE + h.getLen();
                } else {
                    ptr = b.length;
                }
            } else {
                ptr = b.length;
            }
        }
    }

    public void insertSU (Session s, LLT llt) throws IOException, InvocationTargetException, NoSuchMethodException, InternalException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        SystemData sd = new SystemData(Instance.getInstance().getLocalNodeId(),
                                       Instance.getInstance().getFrameSize(),
                                       Instance.getInstance().getFrameSize2(),
                                       Instance.getInstance().getCodePage(),
                                       Instance.getInstance().getDateFormat(),
                                       s.getUser(),
                                       s.getPass(),
                                       Instance.getInstance().getMMPort(),
                                       Instance.getInstance().getRMPort(),
                                       Config.getConfig().FILES_AMOUNT,
                                       new Date(),Instance.SYSTEM_VERSION);
        this.insertChunk(new DataChunk(sd, s), s, true, llt);
    }

    int auth (int user, int pass, Session s) throws NoSuchMethodException, InvocationTargetException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        if (user>0&&pass>0) {
            final SystemData sd = getSystemData();
            if (user==sd.getUser()&&pass==sd.getPasswd()) {
                return Instance.ROOT_USER_ID;
            }
        }
        return 0;
    }

    public SystemData getSystemData() {
        for (Chunk c : data.getChunks()) {
            final DataChunk dc = (DataChunk)c;
            return (SystemData)dc.getEntity();
        }
        return null;
    }

    public int getSystemDBL() {
        return getRes01();
    }

    public void setSystemDBL(int systemDBL) {
        setRes01(systemDBL);
    }

    public int getSystemDBR() {
        return getRes02();
    }

    public void setSystemDBR(int systemDBR) {
        setRes02(systemDBR);
    }

    public int getSysStatus() {
        return getRes03();
    }

    public void setSysStatus(int sysStatus) {
        setRes03(sysStatus);
    }

    public int getExtSize() {
        return getRes04();
    }

    public void setExtSize(int extSize) {
        setRes04(extSize);
    }

    public int getSysVersion() {
        return getRes05();
    }

    public void setSysVersion(int sysVersion) {
        setRes05(sysVersion);
    }

    public long getLastFramePtr() {
        return getRes06();
    }

    public void setLastFramePtr(long lastFramePtr) {
        setRes06(lastFramePtr);
    }

    public long getTblStartFrame() {
        return getRes07();
    }

    public void setTblStartFrame(long tblStartFrame) {
        setRes07(tblStartFrame);
    }

    public long getColStartFrame() {
        return getRes08();
    }

    public void setColStartFrame(long colStartFrame) {
        setRes08(colStartFrame);
    }

}
