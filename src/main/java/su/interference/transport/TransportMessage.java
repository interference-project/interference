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

package su.interference.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.api.SerializerApi;
import su.interference.serialize.CustomSerializer;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class TransportMessage implements Serializable, Delayed {

    public static final int TRANSPORT_MESSAGE = 1;
    public static final int HEARTBEAT_MESSAGE = 2;
    public static final int CALLBACK_MESSAGE = 3;
    private final static Logger logger = LoggerFactory.getLogger(TransportMessage.class);
    private final static Field[] fields = TransportMessage.class.getDeclaredFields();
    private final static SerializerApi sr = new CustomSerializer();
    private final static long serialVersionUID = 8226547655108763221L;

    private final int type;
    private final TransportEvent transportEvent;
    private final TransportCallback transportCallback;
    private final int sender;
    private final String uuid;
    private transient TransportChannel sendChannel;
    private long delayTime;

    protected TransportMessage(int type, int sender, TransportEvent transportEvent, TransportCallback transportCallback) {
        this.type = type;
        this.sender = sender;
        this.transportEvent = transportEvent;
        this.transportCallback = transportCallback;
        if (transportEvent != null) {
            this.uuid = UUID.randomUUID().toString();
            this.transportEvent.setMessageUUID(this.uuid);
        } else {
            this.uuid = transportCallback==null?null:transportCallback.getMessageUUID();
        }
    }

    public TransportMessage() {
        this.type = 0;
        this.sender = 0;
        this.transportEvent = null;
        this.transportCallback = null;
        this.uuid = null;
    }

    @Override
    public long getDelay(TimeUnit timeUnit) {
        final long d = delayTime - System.currentTimeMillis();
        return timeUnit.convert(d, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        TransportMessage message = (TransportMessage)o;

        if (this.getDelayTime() < message.getDelayTime()) {
            return -1;
        }
        if (this.getDelayTime() > message.getDelayTime()) {
            return 1;
        }
        return 0;
    }

    public long getDelayTime() {
        return delayTime;
    }

    public void setDelayTime(long delayTime) {
        this.delayTime = delayTime;
    }

    public int getType() {
        return type;
    }

    public int getSender() {
        return sender;
    }

    public TransportEvent getTransportEvent() {
        return transportEvent;
    }

    public TransportCallback getTransportCallback() {
        return transportCallback;
    }

    public String getUuid() {
        return uuid;
    }

    public String toString() {
        return this.type==TRANSPORT_MESSAGE?"transport":this.type==HEARTBEAT_MESSAGE?"heartbeat":this.type==CALLBACK_MESSAGE?"callback":"unknown";
    }

    public TransportChannel getSendChannel() {
        return sendChannel;
    }

    public void setSendChannel(TransportChannel sendChannel) {
        this.sendChannel = sendChannel;
    }
}
