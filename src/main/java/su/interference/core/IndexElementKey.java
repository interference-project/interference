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

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class IndexElementKey implements Comparable {
    private Object[] key;

    public IndexElementKey (Object[] key) {
        this.key = key;
    }

    // constructor for dataframe pointer
    public IndexElementKey (int p) {
        this.key = new Object[1];
        this.key[0] = p;
    }

    //frame array constructor
    public IndexElementKey (int p1, long p2) {
        this.key = new Object[2];
        this.key[0] = p1;
        this.key[1] = p2;
    }

    @SuppressWarnings("unchecked")
    public int compareTo(final Object obj) {
        final IndexElementKey j = (IndexElementKey)obj;

        for (int i=0; i<key.length; i++) {
            if (key[i] instanceof AtomicInteger) {
                if (((AtomicInteger)this.key[i]).get() < ((AtomicInteger)j.key[i]).get()) { return -1; }
                else if (((AtomicInteger)this.key[i]).get() > ((AtomicInteger)j.key[i]).get()) { return 1; }
            } else if (key[i] instanceof AtomicLong) {
                if (((AtomicLong)this.key[i]).get() < ((AtomicLong)j.key[i]).get()) { return -1; }
                else if (((AtomicLong)this.key[i]).get() > ((AtomicLong)j.key[i]).get()) { return 1; }
            } else {
                return ((Comparable)this.key[i]).compareTo((Comparable)j.key[i]);
            }
        }
        return 0;
    }

    public boolean equals(final Object obj) {
        final IndexElementKey j = (IndexElementKey)obj;
        return this.compareTo(j) == 0;
    }

    public Object[] getKey() {
        return key;
    }

    public void setKey(Object[] key) {
        this.key = key;
    }

    public int hashCode() {
        int hashCode = 1;
        for (Object o : key)
            hashCode = 31 * hashCode + (o == null ? 0 : o.hashCode());
        return hashCode;
    }

}
