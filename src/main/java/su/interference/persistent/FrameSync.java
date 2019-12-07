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

import su.interference.core.DataChunk;
import su.interference.core.DisableSync;
import su.interference.core.IndexColumn;
import su.interference.core.SystemEntity;
import su.interference.exception.InternalException;
import su.interference.mgmt.MgmtColumn;

import javax.persistence.*;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

@Entity
@SystemEntity
@DisableSync
public class FrameSync {
    @Column
    @Id
    @IndexColumn
    @GeneratedValue
    @MgmtColumn(width=20, show=true, form=false, edit=false)
    private long syncId;
    @Column
    @IndexColumn
    @MgmtColumn(width=20, show=true, form=false, edit=false)
    private long allocId;
    @Column
    @IndexColumn
    @MgmtColumn(width=20, show=true, form=false, edit=false)
    private int nodeId;
    @Column
    @IndexColumn
    @MgmtColumn(width=20, show=true, form=false, edit=false)
    private long frameId;
    @Transient
    public static final int CLASS_ID = 15;

    public FrameSync() {

    }

    public FrameSync(long allocId, int nodeId, long frameId) {
        this.allocId = allocId;
        this.nodeId = nodeId;
        this.frameId = frameId;
    }

    //constructor for low-level storage function (initial first-time load table descriptions from datafile)
    public FrameSync(DataChunk chunk) throws ClassNotFoundException, IllegalAccessException, InternalException, MalformedURLException {
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

    public long getSyncId() {
        return syncId;
    }

    public void setSyncId(long syncId) {
        this.syncId = syncId;
    }

    public long getAllocId() {
        return allocId;
    }

    public void setAllocId(long allocId) {
        this.allocId = allocId;
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public long getFrameId() {
        return frameId;
    }

    public void setFrameId(long frameId) {
        this.frameId = frameId;
    }
}
