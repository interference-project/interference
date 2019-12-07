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

import su.interference.core.*;
import su.interference.exception.InternalException;
import su.interference.mgmt.MgmtColumn;
import su.interference.mgmt.MgmtAction;
import su.interference.transport.MgmtEvent;
import su.interference.transport.SQLEvent;
import su.interference.transport.TransportContext;

import javax.persistence.*;
import java.io.Serializable;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.util.Date;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

@Entity
@SystemEntity
@DisableSync
public class Node implements Serializable {
    @Transient
    public static final int NODE_TYPE_MASTER = 1;
    @Transient
    public static final int NODE_TYPE_JOURNAL = 2;
    @Transient
    public static final int NODE_TYPE_SLAVE = 3;
    @Transient
    public static final int NODE_RTYPE_SYNC = 1;
    @Transient
    public static final int NODE_RTYPE_ASYNC = 2;
    @Transient
    public static final int NODE_STATE_ONLINE = Instance.SYSTEM_STATE_ONLINE;
    @Transient
    public static final int NODE_STATE_UP = Instance.SYSTEM_STATE_UP;
    @Transient
    public static final int NODE_STATE_FAIL = Instance.SYSTEM_STATE_FAIL;
    @Transient
    public static final int NODE_STATE_RECOVER = Instance.SYSTEM_STATE_RECOVER;
    @Transient
    public static final int NODE_STATE_DOWN = Instance.SYSTEM_STATE_DOWN;
    @Transient
    public static final int NODE_STATE_IDLE = Instance.SYSTEM_STATE_IDLE;
    @Transient
    public static final int NODE_STATE_NA = Instance.SYSTEM_STATE_NA;

    @Column
    @Id
    @IndexColumn
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int nodeId;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private String host;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int port;
    @Column
    @IndexColumn
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int type;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int rtype;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int state;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int hbeat;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private String token;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int cpuAmount;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private Date lastRated;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int requestRate;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int cpuRate;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int allocRate;

    @Transient
    private int exists;
    @Transient
    public static final int CLASS_ID = 13;
    @Transient
    private final static long serialVersionUID = 8712349857239985123L;

    public static int getCLASS_ID() {
        return CLASS_ID;
    }

    public Node() {

    }

    public Node(int nodeId) {
        this.nodeId = nodeId;
    }

    public Node(int nodeId, String host, int port) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
    }

    //constructor for low-level storage function (initial first-time load table descriptions from datafile)
    public Node (DataChunk chunk) throws ClassNotFoundException, IllegalAccessException, InternalException, MalformedURLException {
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

    @MgmtAction(name="Startup", enable="enableStartup")
    public void startup(Session s) {
        TransportContext.getInstance().send(new MgmtEvent(this.nodeId, MgmtEvent.MGMT_STARTUP));
    }

    @MgmtAction(name="Shutdown", enable="enableShutdown")
    public void shutdown(Session s) {
        TransportContext.getInstance().send(new MgmtEvent(this.nodeId, MgmtEvent.MGMT_SHUTDOWN));
    }

    public void updateNodeState(int newState) {
        if (this.state == NODE_STATE_ONLINE && newState == NODE_STATE_FAIL) {
            //todo shut down all
        }
    }

    public boolean enableStartup() {
        return this.state == NODE_STATE_DOWN;
    }

    public boolean enableShutdown() {
        return this.state == NODE_STATE_UP;
    }

    public boolean enableRegister() {
        return this.state == NODE_STATE_IDLE;
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getRtype() {
        return rtype;
    }

    public void setRtype(int rtype) {
        this.rtype = rtype;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public int getHbeat() {
        return hbeat;
    }

    public void setHbeat(int hbeat) {
        this.hbeat = hbeat;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public int getExists() {
        return exists;
    }

    public void setExists(int exists) {
        this.exists = exists;
    }

    public int getCpuAmount() {
        return cpuAmount;
    }

    public void setCpuAmount(int cpuAmount) {
        this.cpuAmount = cpuAmount;
    }

    public Date getLastRated() {
        return lastRated;
    }

    public void setLastRated(Date lastRated) {
        this.lastRated = lastRated;
    }

    public int getRequestRate() {
        return requestRate;
    }

    public void setRequestRate(int requestRate) {
        this.requestRate = requestRate;
    }

    public int getCpuRate() {
        return cpuRate;
    }

    public void setCpuRate(int cpuRate) {
        this.cpuRate = cpuRate;
    }

    public int getAllocRate() {
        return allocRate;
    }

    public void setAllocRate(int allocRate) {
        this.allocRate = allocRate;
    }

}
