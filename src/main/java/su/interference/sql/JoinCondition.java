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

import su.interference.sqlexception.InvalidCondition;
import su.interference.core.Types;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class JoinCondition extends Condition {
    
    private final SQLColumn conditionColumnRight;
    private int id;

    public JoinCondition(SQLColumn cc, int c, SQLColumn rc, NestedCondition nc) throws InvalidCondition {
        super(cc,c,nc);
        if (!Types.sqlCheck(cc.getColumn().getType().getName(), rc.getColumn().getType().getName())) {
            throw new InvalidCondition();
        }
        this.conditionColumnRight = rc;
    }


    public SQLColumn getConditionColumnRight() {
        return conditionColumnRight;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

}
