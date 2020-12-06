/**
 The MIT License (MIT)

 Copyright (c) 2010-2020 head systems, ltd

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class IndexElementList {
    private CopyOnWriteArrayList<IndexElement> elementList;
    private final int type;
    private int ptr;
    private IndexElementKey maxValue;
    private int parent;
    private int lc;
    private int test;
    private boolean sorted  = false;
    private boolean divided = false;
    private boolean equals  = false;
    private final int MAX_LIST_SIZE = 100;

    public IndexElementList (int type) {
        this.type = type;
        elementList = new CopyOnWriteArrayList<>();
    }

    private void _add (IndexElement e) {
        this.elementList.add(e);
        this.sorted = false;
    }

    public synchronized IndexElementList add (IndexElement e) {
        IndexElementList res = null;
        if (this.isFill()) {
            res = new IndexElementList(this.type);
            res.parent = this.parent;
            if (this.maxValue==null) {
                IndexElementKey max = this.sort();
                if (e.getKey().compareTo(max)>0) {
                    res.elementList.add(e);
                    this.maxValue = max;
                } else {
                    // original
                    this._add(e);
                    this.sort();
                    res.elementList.add(this.elementList.get(this.elementList.size()-1));
                    this.elementList.remove(this.elementList.size()-1);
                    this.maxValue = this.elementList.get(this.elementList.size()-1).getKey();
                }
                res.lc = this.lc;
                this.lc = 0;
            } else {
                if (e.getKey().compareTo(this.maxValue)>0) {
                    throw new RuntimeException("internal error");
                } else {
                    res.divided = true;
                    this._add(e);
                    this.sort(); //
                    CopyOnWriteArrayList<IndexElement> nlist = new CopyOnWriteArrayList<IndexElement>();
                    IndexElementKey pkey = null;
                    boolean keyrpt = false;
                    boolean norpt  = false;
                    for (int i=0; i<this.elementList.size(); i++) {
                        if (!norpt) {
                            if (pkey!=null) {
                                if (this.elementList.get(i).getKey().compareTo(pkey)==0) {
                                    keyrpt = true;
                                }
                            } else {
                                keyrpt = true;
                            }
                        }
                        if (i>=this.MAX_LIST_SIZE) {
                            keyrpt = false;
                        }
                        int half = this.MAX_LIST_SIZE/2;
                        if (keyrpt||(this.elementList.get(i).getKey().compareTo(this.maxValue)<0&&i<=half)) {
                            res.elementList.add(this.elementList.get(i));
                            res.maxValue = this.elementList.get(i).getKey();
                        } else {
                            norpt = true;
                            nlist.add(this.elementList.get(i));
                        }
                        pkey = this.elementList.get(i).getKey();
                        keyrpt = false;
                    }
                    this.elementList = nlist; //update element list in current IEL

                }
            }
        } else {
            this._add(e);
        }
        return res;
    }

    public IndexElement get(final int index) {
        return this.elementList.get(index);
    }

    private boolean isFill() {
        return this.elementList.size() >= MAX_LIST_SIZE;
    }

    @SuppressWarnings("unchecked")
    public synchronized IndexElementKey sort() {
        Collections.sort(this.elementList);
        this.sorted = true;
        if (this.elementList.size()>0) {
            return this.elementList.get(this.elementList.size()-1).getKey();
        } else {
            return null;
        }
    }

    //accepted only to node element lists
    //for unique indexes
    public synchronized int getChildElementPtr(final IndexElementKey key) {
        if (!this.sorted) {
            this.sort();
        }
        for (IndexElement ie : this.elementList) {
            if (ie.getKey().compareTo(key)>=0) {
                return (Integer)ie.getElement(); //known as ptr for node element
            }
        }
        return -1;
    }

    //accepted only to node element lists
    //for non-unique indexes
    public synchronized ArrayList<Integer> getChildElementsPtr(final IndexElementKey key) {
        if (!this.sorted) {
            this.sort();
        }
        final ArrayList<Integer> r = new ArrayList<Integer>();
        //boolean f = false;
        for (IndexElement ie : this.elementList) {
            if (ie.getKey().compareTo(key)==0) {
                r.add ((Integer)ie.getElement()); //known as ptr for node element
                //f = true;
            }
            if (ie.getKey().compareTo(key)>0) {
                //if (!f) {
                    r.add ((Integer)ie.getElement()); //known as ptr for node element
                //}
                break;
            }
        }
        return r;
    }

    //return first element which found - for unique indexes
    public synchronized Object getObjectByKey(final IndexElementKey key) {
        for (IndexElement ie : this.elementList) {
            if (ie.getKey().equals(key)) {
                return ie.getElement();
            }
        }
        return null;
    }

    //return all element which found - for non-unique indexes
    public synchronized List<Object> getObjectsByKey(final IndexElementKey key) {
        final ArrayList<Object> r = new ArrayList<>();
        for (IndexElement ie : this.elementList) {
            if (ie.getKey().equals(key)) {
                r.add(ie.getElement());
            }
        }
        return r;
    }

    public synchronized void updateObjects(IndexElementKey key, Object o) {
        int d = -1;
        for (int i=0; i<this.elementList.size(); i++) {
            IndexElement ie = this.elementList.get(i);
            if (ie.getKey().equals(key)) {
                ie.setElement(o);
            }
        }
    }

    public synchronized void removeObjects(IndexElementKey key, Object o) {
        int d = -1;
        for (int i=0; i<this.elementList.size(); i++) {
            IndexElement ie = this.elementList.get(i);
            if (ie.getKey().equals(key)) {
                if (ie.getElement()==o) {
                    d = i;
                }
            }
        }
        if (d>=0) { this.elementList.remove(d); }
    }

    public synchronized CopyOnWriteArrayList<IndexElement> getElementList() {
        return elementList;
    }

    public int getType() {
        return type;
    }

    public int getPtr() {
        return ptr;
    }

    public void setPtr(int ptr) {
        this.ptr = ptr;
    }

    public IndexElementKey getMaxValue() {
        return maxValue;
    }

    public int getParent() {
        return parent;
    }

    public void setParent(int parent) {
        this.parent = parent;
    }

    public int getLc() {
        return lc;
    }

    public void setLc(int lc) {
        this.lc = lc;
    }

    public boolean isDivided() {
        return divided;
    }

    public boolean isEquals() {
        return equals;
    }

    public void setEquals(boolean equals) {
        this.equals = equals;
    }

    public int getTest() {
        return test;
    }

    public void setTest(int test) {
        this.test = test;
    }

}
