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

import java.io.Serializable;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class TransportCallback implements Serializable {
    public static final int SUCCESS = 0;
    public static final int FAILURE = 1;

    private final int nodeId;
    private final String messageUUID;
    private final EventResult result;

    public TransportCallback(int nodeId, String messageUUID, EventResult result) {
        this.nodeId = nodeId;
        this.messageUUID = messageUUID;
        this.result = result;
    }

    public int getNodeId() {
        return nodeId;
    }

    public String getMessageUUID() {
        return messageUUID;
    }

    public EventResult getResult() {
        return result;
    }
}
