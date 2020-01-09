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

package su.interference.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class MgmtEvent extends TransportEventImpl {

    private final static long serialVersionUID = 436398796431755011L;
    private final static Logger logger = LoggerFactory.getLogger(MgmtEvent.class);
    public final static int MGMT_STARTUP = 1;
    public final static int MGMT_SHUTDOWN = 2;
    private int command;

    public MgmtEvent(int channelId, int command) {
        super(channelId);
        this.command = command;
    }

    @Override
    public EventResult process() {
        return new EventResult(TransportCallback.SUCCESS, 0, null, null);
    }
}