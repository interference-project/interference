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
import su.interference.core.IndexColumn;
import su.interference.core.DataChunk;
import su.interference.mgmt.MgmtColumn;
import su.interference.exception.InternalException;

import javax.persistence.Entity;
import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Transient;
import java.net.MalformedURLException;
import java.lang.reflect.Modifier;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

@Entity
@SystemEntity
public class MgmtModule {
    @Column
    @Id
    @IndexColumn
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int moduleId;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private String name;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private String className;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int accessId;

    @Transient
    public static final int CLASS_ID = 12;

    public static int getCLASS_ID() {
        return CLASS_ID;
    }

    public MgmtModule() {

    }

    public MgmtModule(int moduleId, String name, String className, int accessId) {
        this.moduleId = moduleId;
        this.name = name;
        this.className = className;
        this.accessId = accessId;
    }

    //constructor for low-level storage function (initial first-time load table descriptions from datafile)
    public MgmtModule (DataChunk chunk) throws IllegalAccessException, ClassNotFoundException, InternalException, MalformedURLException {
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

    public int getModuleId() {
        return moduleId;
    }

    public void setModuleId(int moduleId) {
        this.moduleId = moduleId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public int getAccessId() {
        return accessId;
    }

    public void setAccessId(int accessId) {
        this.accessId = accessId;
    }

}
