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

package su.interference.sql;

import su.interference.persistent.Node;
import su.interference.transport.SQLEvent;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SQLNode {

    private final int nodeId;
    private final long slaveCursorId;
    private final Node node;
    private final int type;
    private int taskComplete;
    private SQLEvent sqlEvent;

    public static final int NODE_TYPE_MASTER = 1;
    public static final int NODE_TYPE_SLAVE = 2;

    public SQLNode(int nodeId, int type, long slaveCursorId) {
        this.nodeId = nodeId;
        this.node = null;
        this.type = type;
        this.slaveCursorId = slaveCursorId;
    }

/*
    public SQLNode(Node node, int type) {
        this.node = node;
        this.type = type;
    }
*/

    public int getNodeId() {
        return nodeId;
    }

/*
    public Node getNode() {
        return node;
    }
*/

    public int getType() {
        return type;
    }

}
