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

import java.util.List;
import java.util.ArrayList;
import java.util.function.Function;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SQLGroupFunction {

    private final List<Object> fos;
    private final int f;
    private int cnt;
    private long sum;
    private long avg;
    private Comparable min;
    private Comparable max;
    private Object last;

    public static final int F_COUNT  = 100;
    public static final int F_SUM    = 101;
    public static final int F_MIN    = 102;
    public static final int F_MAX    = 103;
    public static final int F_AVG    = 104;
    public static final int F_LAST   = 104;
    public static final int F_CUSTOM = 199;

    public SQLGroupFunction (int f) {
        fos = new ArrayList<Object>();
        this.f = f;
    }

    public SQLGroupFunction (Function f) {
        fos = new ArrayList<Object>();
        this.f = F_CUSTOM;
    }

    @SuppressWarnings("unchecked")
    public void add (Object o) {
        if (f==F_COUNT || f==F_AVG) { cnt++; }
        if (f==F_SUM || f==F_AVG)   { sum = sum + checkNum(o); }
        if (f==F_MIN)  { if (min==null) { min = checkComparable(o); } else { min = min.compareTo(checkComparable(o))<0?min:checkComparable(o); }}
        if (f==F_MAX)  { if (max==null) { max = checkComparable(o); } else { max = max.compareTo(checkComparable(o))>0?max:checkComparable(o); }}
        if (f==F_AVG)  { avg = sum/cnt; }
        if (f==F_LAST) { last = o; }
        //fos.add(o);
    }

    public Object getResult() {
        if (f==F_COUNT) { return cnt; }
        if (f==F_SUM)   { return sum; }
        if (f==F_MIN)   { return min; }
        if (f==F_MAX)   { return max; }
        if (f==F_AVG)   { return avg; }
        if (f==F_LAST)  { return last; }
        return null;
    }

    public void clear() {
        cnt = 0;
        sum = 0;
        avg = 0;
        min = null;
        max = null;
        last = null;
    }

    private long checkNum(Object o) {
        if (o.getClass().getName().equals("java.lang.Long")) {
            return (Long)o;
        }
        if (o.getClass().getName().equals("java.lang.Integer")) {
            return (long) (Integer)o;
        }
        return 0L;
    }

    private Comparable checkComparable (Object o) {
        for (Class<?> i : o.getClass().getInterfaces()) {
            if (i.getName().equals("java.lang.Comparable")) {
                return (Comparable)o;
            }
        }
        return null;
    }

}
