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

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

import su.interference.core.DataChunk;
import su.interference.core.SystemEntity;
import su.interference.core.IndexColumn;
import su.interference.exception.InternalException;

import java.io.UnsupportedEncodingException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;

import javax.persistence.Entity;
import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Transient;

/**
 * Created by IntelliJ IDEA.
 * User: glotanov
 * Date: 20.01.2011
 * Time: 16:33:01
 * To change this template use File | Settings | File Templates.
 */

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

@Entity
@SystemEntity
public class Field implements Comparable {

    @Id
    @Column
    @IndexColumn
    private int    columnId;
    @Column
    private int    objectId;
    @Column
    private String name;
    @Column
    private String dataType;
    @Column
    private int    notNull;
    @Transient
    public static final int CLASS_ID = 2;

    public Field () {

    }

    public Field (int objectId, String name, String dataType) {
        this.setObjectId(objectId);
        this.setName(name);
        this.setDataType(dataType);
    }

    public Field (int objectId, Field c) {
        this.setObjectId(objectId);
        this.setName(c.getName());
        this.setDataType(c.getDataType());
        this.setNotNull(0);
    }

    //constructor for low-level storage function (initial first-time load table descriptions from datafile)
    public Field (DataChunk chunk) throws ClassNotFoundException, IllegalAccessException, InternalException, MalformedURLException {
        Object[] dcs = chunk.getDcs().getValueSet();
        Class c = this.getClass();
        Entity ca = (Entity)c.getAnnotation(Entity.class);
        java.lang.reflect.Field[] f = c.getDeclaredFields();
        int x = 0;
        for (int i=0; i<f.length; i++) {
            Column fa = f[i].getAnnotation(Column.class);
            Transient ta = f[i].getAnnotation(Transient.class);
            if (ta==null) {
                int m = f[i].getModifiers();
                if (Modifier.isPrivate(m)) {
                    f[i].setAccessible(true);
                }
                f[i].set(this, dcs[x]);
                x++;
            }
        }
    }

    public long getId () {
        return this.columnId;
    }

    public int compareTo(Object obj) {
        Field c = (Field)obj;
        if(this.getColumnId() < c.getColumnId()) {
            return -1;
        } else if(this.getColumnId() > c.getColumnId()) {
            return 1;
        }
        return 0;
    }

    public int getColumnId() {
        return columnId;
    }

    public void setColumnId(int columnId) {
        this.columnId = columnId;
    }

    public int getObjectId() {
        return objectId;
    }

    public void setObjectId(int objectId) {
        this.objectId = objectId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public int getNotNull() {
        return notNull;
    }

    public void setNotNull(int notNull) {
        this.notNull = notNull;
    }

}
