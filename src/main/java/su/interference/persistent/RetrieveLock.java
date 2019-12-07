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

import su.interference.core.SystemEntity;
import su.interference.core.DataChunk;
import su.interference.core.IndexColumn;
import su.interference.mgmt.MgmtColumn;
import su.interference.exception.InternalException;

import javax.persistence.Entity;
import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Transient;
import java.util.ArrayList;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

@Entity
@SystemEntity
public class RetrieveLock {

    @Column
    @IndexColumn
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int objectId;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private long transId; //lock transactions
    @Id
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    @Transient
    private String lockId;
    @Transient
    public static final int CLASS_ID = 11;

    public String getLockId() {
        return getHexByLong(transId)+getHexByInt(objectId);
    }

    public RetrieveLock () {

    }

    public RetrieveLock (int obj, long tran) {
        this.objectId = obj;
        this.transId  = tran;
    }

    //constructor for low-level storage function (initial first-time load table descriptions from datafile)
    public RetrieveLock (DataChunk chunk) throws ClassNotFoundException, IllegalAccessException, InternalException, MalformedURLException {
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

    public int getObjectId() {
        return objectId;
    }

    public void setObjectId(int objectId) {
        this.objectId = objectId;
    }

    public long getTransId() {
        return transId;
    }

    public void setTransId(long transId) {
        this.transId = transId;
    }

    public String getHexByInt (int val) {
       String s = Integer.toHexString(val);
       String p = new String();
       for (int i=0; i < 8-s.length(); i++) {
           p = p + "0";
       }
       return p + s;
    }

    public String getHexByLong (long val) {
       String s = Long.toHexString(val);
       String p = new String();
       for (int i=0; i < 16-s.length(); i++) {
           p = p + "0";
       }
       return p + s;
    }


}
