/**
 The MIT License (MIT)

 Copyright (c) 2010-2021 head systems, ltd

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
import su.interference.core.IndexColumn;
import su.interference.core.MapColumn;
import su.interference.core.SystemEntity;
import su.interference.mgmt.MgmtColumn;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Transient;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

@Entity
@SystemEntity
@DisableSync
public class EventSubscriber {

    @Id
    @Column
    @MapColumn
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private String entityId;
    @Column
    @IndexColumn
    @MgmtColumn(width=20, show=true, form=false, edit=false)
    private String subscriberId;

    @Transient
    public static final int CLASS_ID = 16;

    public static int getCLASS_ID() {
        return CLASS_ID;
    }

    public EventSubscriber() {

    }

    public EventSubscriber(String entityId, String subscriberId) {
        this.entityId = entityId;
        this.subscriberId = subscriberId;
    }

    public String getEntityId() {
        return entityId;
    }

    public String getSubscriberId() {
        return subscriberId;
    }
}
