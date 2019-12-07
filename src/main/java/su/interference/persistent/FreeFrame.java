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

package su.interference.persistent;

import su.interference.core.DisableSync;
import su.interference.mgmt.MgmtColumn;
import su.interference.core.SystemEntity;
import su.interference.core.DataChunk;
import su.interference.core.IndexColumn;
import su.interference.exception.InternalException;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Transient;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

@Entity
@SystemEntity
@DisableSync
public class FreeFrame implements Comparable, FilePartitioned {

    @Id
    @Column
    @IndexColumn
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private long frameId;
    @Column
    @IndexColumn
    @MgmtColumn(width=20, show=true, form=false, edit=false)
    private int fileId;
    @Column
    @MgmtColumn(width=20, show=true, form=false, edit=false)
    private int objectId;
    @Column
    @MgmtColumn(width=30, show=true, form=false, edit=false)
    private int size;
    @Column
    @MgmtColumn(width=30, show=true, form=false, edit=false)
    private int passed;

    @Transient
    public static final int CLASS_ID = 5;

    public static int getCLASS_ID() {
        return CLASS_ID;
    }

    public FreeFrame() {
        
    }

    public FreeFrame(int objectId, long frame, int size) {
        this.objectId = objectId;
        this.frameId  = frame;
        this.size     = size;
        this.fileId   = (int)frame%4096;
    }

    public int compareTo(Object obj) {
        FreeFrame c = (FreeFrame)obj;
        if (this.frameId < c.getFrameId()) {
            return -1;
        } else if (this.frameId > c.getFrameId()) {
            return 1;
        }
        return 0;
    }

    //constructor for low-level storage function (initial first-time load table descriptions from datafile)
    public FreeFrame(DataChunk chunk) throws ClassNotFoundException, IllegalAccessException, InternalException, MalformedURLException {
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

    public int getFileId() {
        return fileId;
    }

    public int hashCode() {
        return Long.valueOf(this.frameId).hashCode();
    }

    public int getFile() {
        return (int)this.frameId%4096;
    }

    public long getPtr() {
        return this.frameId - (this.frameId%4096);
    }

    public long getFrameId() {
        return frameId;
    }

    public void setFrameId(long frameId) {
        this.frameId = frameId;
    }

    public int getObjectId() {
        return objectId;
    }

    public void setObjectId(int objectId) {
        this.objectId = objectId;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getPassed() {
        return passed;
    }

    public void setPassed(int passed) {
        this.passed = passed;
    }
}
