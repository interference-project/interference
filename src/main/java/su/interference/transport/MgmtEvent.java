/**
 The MIT License (MIT)

 Copyright (c) 2010-2025 head systems, ltd

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

package su.interference.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.core.Instance;
import su.interference.persistent.Session;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class MgmtEvent extends TransportEventImpl {

    private final static long serialVersionUID = 436398796431755011L;
    private final static Logger logger = LoggerFactory.getLogger(MgmtEvent.class);
    public final static int MGMT_STARTUP = 1;
    public final static int MGMT_SHUTDOWN = 2;
    public final static int MGMT_GETSTATE = 3;
    private int command;
    private String sessionId;

    public MgmtEvent(int channelId, int command, String sessionId) {
        super(channelId);
        this.command = command;
        this.sessionId = sessionId;
    }

    @Override
    public EventResult process() {
        try {
            Session s = Instance.getInstance().getSession(sessionId);
            if (s == null) {
                s = Session.getSession(this.channelId, this.sessionId);
            }
            if (this.command == MGMT_GETSTATE) {
                return new EventResult(TransportCallback.SUCCESS, Instance.getInstance().getSystemState(), 0, null, null, null);
            } else if (this.command == MGMT_STARTUP) {
                Instance.getInstance().startupDatabase(s);
                return new EventResult(TransportCallback.SUCCESS, 0, 0, null, null, null);
            } else if (this.command == MGMT_SHUTDOWN) {
                Instance.getInstance().shutdownDatabase(s);
                return new EventResult(TransportCallback.SUCCESS, 0, 0, null, null, null);
            } else {
                return new EventResult(TransportCallback.SUCCESS, null, 0, null, null, null);
            }
        } catch (Exception e) {
            return new EventResult(TransportCallback.FAILURE, e.getMessage(), 0, null, null, null);
        }
    }
}