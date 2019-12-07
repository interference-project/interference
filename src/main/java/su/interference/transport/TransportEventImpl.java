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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class TransportEventImpl implements TransportEvent {
    private final static Logger logger = LoggerFactory.getLogger(TransportEventImpl.class);
    private final static long serialVersionUID = 4363987967075461209L;
    protected final int channelId;
    private String messageUUID;
    private int callbackNodeId;
    private TransportCallback callback;
    private transient CountDownLatch latch;
    private AtomicBoolean sent = new AtomicBoolean(false);
    private List<Integer> failures = new ArrayList<>();
    private Exception processException;

    public TransportEventImpl(int channelId) {
        this.channelId = channelId;
    }

    public int getChannelId() {
        return channelId;
    }

    public boolean isBroadcast() {
        return channelId == 0;
    }

    public String getMessageUUID() {
        return messageUUID;
    }

    public void setMessageUUID(String uuid) {
        this.messageUUID = uuid;
    }

    public int getCallbackNodeId() {
        return callbackNodeId;
    }

    public void setCallbackNodeId(int callbackNodeId) {
        this.callbackNodeId = callbackNodeId;
    }

    public TransportCallback getCallback() {
        return callback;
    }

    public void setCallback(TransportCallback callback) {
        this.callback = callback;
    }

    public CountDownLatch getLatch() {
        return latch;
    }

    public void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }

    public final boolean isSent() {
        return sent.get();
    }

    public final void sent() {
        this.sent.set(true);
    }

    public final boolean isFail() {
        return failures.size() > 0;
    }

    public final void failure(int channelId, Exception e) {
        failures.add(channelId);
        processException = e;
    }

    public EventResult process() { return null; }

    public Exception getProcessException() {
        return processException;
    }

}
