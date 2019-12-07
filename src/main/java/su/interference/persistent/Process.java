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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.core.*;
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
public class Process implements Comparable {

    @Column
    @Id
    @IndexColumn
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int processId;

    @Column
    @IndexColumn
    @MgmtColumn(width=40, show=true, form=false, edit=false)
    private String processName;

    @Column
    @MgmtColumn(width=40, show=true, form=false, edit=false)
    private String className;

    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private String state;

    @Transient
    private Thread   th;
    @Transient
    private Runnable ro;
    @Transient
    public static final int CLASS_ID = 10;
    @Transient
    private final static Logger logger = LoggerFactory.getLogger(Process.class);

    public static int getCLASS_ID() {
        return CLASS_ID;
    }

    public void start (Runnable r, Session s) {
        this.ro = r;
        this.th = new Thread(r);
        this.th.start();
        Thread.State ts = this.th.getState();
        this.state = ts.name();
        try {
            s.persist(this); //update
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void stop () throws InterruptedException {
        if (th!=null&&ro!=null) {
            ((ManageProcess)ro).stop();
            th.join();
        }
    }

    public Process () {

    }

    public Process (Runnable r) {
        this.ro = r;
    }

    //system init constructor
    public Process(int processId, String processName, String className) {
        this.ro = null;
        this.processId = processId;
        this.processName = processName;
        this.className = className;
    }

    //constructor for low-level storage function (initial first-time load table descriptions from datafile)
    public Process (DataChunk chunk) throws IllegalAccessException, ClassNotFoundException, InternalException, MalformedURLException {
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

    public int compareTo(Object obj) {
        Process p = (Process)obj;
        if (this.processId < p.processId) {
            return -1;
        }
        if (this.processId > p.processId) {
            return 1;
        }
        return 0;
    }

    public Runnable getRunnable() {
        return ro;
    }

    public int getProcessId() {
        return processId;
    }

    public void setProcessId(int processId) {
        this.processId = processId;
    }

    public String getProcessName() {
        return processName;
    }

    public void setProcessName(String processName) {
        this.processName = processName;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

}
