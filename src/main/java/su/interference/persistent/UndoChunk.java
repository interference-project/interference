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
import su.interference.core.SystemEntity;
import su.interference.core.DataChunk;
import su.interference.core.RowId;
import su.interference.mgmt.MgmtColumn;
import su.interference.exception.InternalException;

import javax.persistence.Entity;
import javax.persistence.Column;
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
public class UndoChunk implements FilePartitioned {

    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private long transId;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int file;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private long frame;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int ptr;
    @Column
    private DataChunk dataChunk;
    @Id
    @Transient
    private long chunkId;
    @Transient
    private Transaction tran;
    @Transient
    public static final int CLASS_ID = 9;

    public static int getCLASS_ID() {
        return CLASS_ID;
    }

    public UndoChunk() {

    }

    public UndoChunk (DataChunk dc, Transaction tr, int f, long b, int p) {
        this.dataChunk = dc;
        this.transId   = tr.getTransId();
        this.file      = f;
        this.frame       = b;
        this.ptr       = p;
        this.tran      = tr;
    }

    //constructor for low-level storage function (initial first-time load table descriptions from datafile)
    public UndoChunk (DataChunk chunk) throws IllegalAccessException, ClassNotFoundException, InternalException, MalformedURLException {
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

    public long getChunkId() {
        return chunkId;
    }

    public long getTransId() {
        return transId;
    }

    public void setTransId(long transId) {
        this.transId = transId;
    }

    public DataChunk getDataChunk() {
        return dataChunk;
    }

    public void setDataChunk(DataChunk dataChunk) {
        this.dataChunk = dataChunk;
    }

    public int getFile() {
        return file;
    }

    public void setFile(int file) {
        this.file = file;
    }

    public long getFrame() {
        return frame;
    }

    public void setFrame(long frame) {
        this.frame = frame;
    }

    public int getPtr() {
        return ptr;
    }

    public void setPtr(int ptr) {
        this.ptr = ptr;
    }

}
