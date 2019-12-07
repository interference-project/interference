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

import javax.persistence.Entity;
import javax.persistence.Column;
import java.util.Date;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

@Entity
@SystemEntity
public class SystemData {

    @Column
    private final int nodeId;
    @Column
    private final int framesize;
    @Column
    private final int framesize2;
    @Column
    private final String cp;
    @Column
    private final String df;
    @Column
    private final int user;
    @Column
    private final int passwd;
    @Column
    private final int mmport;
    @Column
    private final int mmport2;
    @Column
    private final int datafiles;
    @Column
    private final Date initDate;
    @Column
    private final int version;

    public SystemData() {
        this.nodeId = 0;
        this.framesize = 0;
        this.framesize2 = 0;
        this.cp = null;
        this.df = null;
        this.user = 0;
        this.passwd = 0;
        this.mmport = 0;
        this.mmport2 = 0;
        this.datafiles = 0;
        this.initDate = null;
        this.version = 0;
    }

    public SystemData(int nodeId, int framesize, int framesize2, String cp, String df, int user, int passwd, int mmport, int datafiles, int mmport2, Date initDate, int version) {
        this.nodeId = nodeId;
        this.framesize = framesize;
        this.framesize2 = framesize2;
        this.cp = cp;
        this.df = df;
        this.user = user;
        this.passwd = passwd;
        this.mmport = mmport;
        this.mmport2 = mmport2;
        this.datafiles = datafiles;
        this.initDate = initDate;
        this.version = version;
    }

    public int getNodeId() {
        return nodeId;
    }

    public int getUser() {
        return user;
    }

    public int getPasswd() {
        return passwd;
    }

    public Date getInitDate() {
        return initDate;
    }

    public int getFramesize() {
        return framesize;
    }

    public int getFramesize2() {
        return framesize2;
    }

    public String getCp() {
        return cp;
    }

    public String getDf() {
        return df;
    }

    public int getVersion() {
        return version;
    }

    public int getMmport() {
        return mmport;
    }

    public int getMmport2() {
        return mmport2;
    }

    public int getDatafiles() {
        return datafiles;
    }
}
