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

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class Condition {

    public static final int C_EQUAL = 1;
    public static final int C_NOT_EQUAL = 2;
    public static final int C_LESS = 3;
    public static final int C_MORE = 4;
    public static final int C_LESS_EQUAL = 5;
    public static final int C_MORE_EQUAL = 6;
    public static final int C_IN = 7;
    public static final int C_NOT_IN = 8;
    public static final int C_LIKE = 9;
    public static final int C_NOT_LIKE = 10;

    private final SQLColumn conditionColumn;
    private int condition; //equal, not equal, less, more, less equal, nore equal, in, not in, like
    private NestedCondition nc;

    public Condition () {
        conditionColumn = null;
    }

    public Condition (SQLColumn cc, int c, NestedCondition nc) {
        this.conditionColumn = cc;
        this.condition = c;
        this.nc = nc;
    }

    public static boolean isNumericCondition (int ctype) {
        if ((ctype == C_EQUAL)||(ctype == C_NOT_EQUAL)||(ctype == C_IN)||(ctype == C_NOT_IN)||(ctype == C_LESS)||(ctype == C_MORE)||(ctype == C_LESS_EQUAL)||(ctype == C_MORE_EQUAL)) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean isStringCondition (int ctype) {
        if ((ctype == C_EQUAL)||(ctype == C_NOT_EQUAL)||(ctype == C_IN)||(ctype == C_NOT_IN)||(ctype == C_LIKE)||(ctype == C_NOT_LIKE)) {
            return true;
        } else {
            return false;
        }
    }

    public SQLColumn getConditionColumn() {
        return conditionColumn;
    }

    public int getCondition() {
        return condition;
    }

    public void setCondition(int condition) {
        this.condition = condition;
    }

    public NestedCondition getNc() {
        return nc;
    }

    public void setNc(NestedCondition nc) {
        this.nc = nc;
    }
    
}
