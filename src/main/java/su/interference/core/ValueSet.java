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

package su.interference.core;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class ValueSet implements Comparable {

    private final Object[] vs;

    public ValueSet(Object v) {
        this.vs = new Object[]{v};
    }

    public ValueSet(Object[] vs) {
        this.vs = vs;
    }

    public Object[] getValueSet() {
        return vs;
    }

    public int compareTo (Object obj) {
        return compare(obj, vs.length);
    }

    //used in partial compare datachunks in sql group algorithm
    //thr = threshold for set (actual fields amount)
    @SuppressWarnings("unchecked")
    public int compare (Object obj, int thr) {
        final ValueSet j = (ValueSet)obj;

        for (int i=0; i<vs.length; i++) {
            final int ct = ((Comparable)this.vs[i]).compareTo(j.getValueSet()[i]);
            if (ct != 0) {
                return ct;
            }
            if (i==thr-1) {
                break;
            }
        }
        return 0;
    }

    public boolean equals (Object obj) {
        final ValueSet j = (ValueSet)obj;
        return this.compareTo(j) == 0;
    }

    public int hashCode() {
        int hashCode = 1;
        for (Object o : vs)
            hashCode = 31 * hashCode + (o == null ? 0 : o.hashCode());
        return hashCode;
    }

}
