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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class IndexList {

    private CopyOnWriteArrayList<IndexElementList> list;
    private int start = 0; //current start EL

    public IndexList () {
        list = new CopyOnWriteArrayList<IndexElementList>();
        addElementList(new IndexElementList(1));
    }

    public int size() {
        return list.size();
    }

    public synchronized void add (int obj, Object element) {
        add (new IndexElementKey(new Integer[]{obj}), element);
    }

    public synchronized void add (long id, Object element) {
        add (new IndexElementKey(new Long[]{id}), element);
    }

    public synchronized void add (String id, Object element) {
        add (new IndexElementKey(new String[]{id}), element);
    }

    public synchronized void add (IndexElementKey key, Object element) {
        IndexElement e = new IndexElement(key, element);

        boolean cnue = true;
        IndexElementList target = list.get(start);

        //find target leaf
        while (cnue) {
            if (target.getType()==1) { //leaf
                cnue = false;
            } else {                   //node
                int cptr   = target.getChildElementPtr(key);
                int parent = target.getPtr();
                if (cptr>=0) {
                    target = list.get(cptr);
                } else {
                    target = list.get(target.getLc()); //get by last child
                }
                target.setParent(parent);
            }
        }
        cnue = true;

        //store element to target leaf
        IndexElementList prevtg;
        while (cnue) {
            IndexElementList newlist = target.add(e);
            if (newlist==null) {
                cnue = false;
            } else {
                addElementList(newlist);
                prevtg = target;
                if (newlist.isDivided()) {
                    e = new IndexElement(newlist.getMaxValue(), new Integer(newlist.getPtr()));
                } else {
                    e = new IndexElement(prevtg.getMaxValue(), new Integer(prevtg.getPtr()));
                }
                if (prevtg.getParent()==0) { //add parent ElementList - always type 2 (node)
                    target = new IndexElementList(2);
                    addElementList(target);
                    this.start = target.getPtr();
                } else {
                    target = list.get(prevtg.getParent());
                }
                if (!newlist.isDivided()) {
                    target.setLc(newlist.getPtr()); //lc must be > 0 (0 is first leaf ElementList)
                }
            }
        }

    }

    public synchronized void update (IndexElementKey key, Object o) {
        updateObjects(key, o);
    }

    public synchronized void remove (IndexElementKey key, Object o) {
        removeObjects(key, o);
    }

    public synchronized List<Object> getObjectsByKey (final IndexElementKey key) {
        final ArrayList<Object> r = new ArrayList<Object>();
        boolean cnue = true;
        ArrayList<IndexElementList> targets = new ArrayList<IndexElementList>();
        IndexElementList el = list.get(start);
        //el.sort();
        targets.add(el);
        while (cnue) {
            final ArrayList<IndexElementList> ntargets = new ArrayList<IndexElementList>();
            for (IndexElementList target : targets) {
                //target.sort();
                if (target.getType()==1) { //leaf
                    r.addAll(target.getObjectsByKey(key));
                    cnue = false;
                } else {
                    final ArrayList<Integer> cptr = target.getChildElementsPtr(key);
                    if (cptr.size()>0) {
                        for (Integer i : cptr) {
                            ntargets.add(list.get(i));
                        }
                    }
                    if (target.getLc()>0) {
                        ntargets.add(list.get(target.getLc())); //get by last child
                    }
                }
            }
            targets = ntargets;
        }
        return r;
    }

    public synchronized List<Object> getContent() {
        ArrayList<Object> res = new ArrayList<Object>();
        ArrayList<IndexElementList> levelNodes = new ArrayList<IndexElementList>();
        boolean cnue = true;
        IndexElementList el = list.get(start);
        el.sort();
        levelNodes.add(el);
        while (cnue) {
            ArrayList<IndexElementList> inNodes = new ArrayList<IndexElementList>();
            int lc = 0;
            for (int k=0; k<levelNodes.size(); k++) {
                levelNodes.get(k).sort();
                if (levelNodes.get(k).getType()==1) {
                    cnue = false;
                    for (IndexElement ie : levelNodes.get(k).getElementList()) {
                        if (levelNodes.get(k).getType()==1) {
                            res.add(ie.getElement());
                        }
                    }
                } else {
                    for (int i=0; i<levelNodes.get(k).getElementList().size(); i++) {
                        inNodes.add(this.list.get((Integer)levelNodes.get(k).getElementList().get(i).getElement()));
                    }
                    if (k==levelNodes.size()-1) {
                        lc = levelNodes.get(k).getLc();
                        if (lc>0) {
                            inNodes.add(this.list.get(lc));
                        }
                    }
                }
            }
            levelNodes = inNodes;
        }
        return res;
    }

    public synchronized Object getFirst() {
        ArrayList<IndexElementList> levelNodes = new ArrayList<IndexElementList>();
        boolean cnue = true;
        IndexElementList el = list.get(start);
        el.sort();
        levelNodes.add(el);
        while (cnue) {
            ArrayList<IndexElementList> inNodes = new ArrayList<IndexElementList>();
            int lc = 0;
            for (int k=0; k<levelNodes.size(); k++) {
                levelNodes.get(k).sort();
                if (levelNodes.get(k).getType()==1) {
                    cnue = false;
                    for (IndexElement ie : levelNodes.get(k).getElementList()) {
                        if (levelNodes.get(k).getType()==1) {
                            return ie.getElement();
                        }
                    }
                } else {
                    for (int i=0; i<levelNodes.get(k).getElementList().size(); i++) {
                        inNodes.add(this.list.get((Integer)levelNodes.get(k).getElementList().get(i).getElement()));
                    }
                    if (k==levelNodes.size()-1) {
                        lc = levelNodes.get(k).getLc();
                        if (lc>0) {
                            inNodes.add(this.list.get(lc));
                        }
                    }
                }
            }
            levelNodes = inNodes;
        }
        return null;
    }

    public synchronized String getInfo() {
        ArrayList<IndexElementList> levelNodes = new ArrayList<IndexElementList>();
        boolean cnue = true;
        IndexElementList el = list.get(start);
        el.sort();
        levelNodes.add(el);
        int nodecnt = 0;
        int leafcnt = 0;
        int nodeamt = 0;
        int leafamt = 0;
        while (cnue) {
            ArrayList<IndexElementList> inNodes = new ArrayList<IndexElementList>();
            int lc = 0;
            for (int k=0; k<levelNodes.size(); k++) {
                levelNodes.get(k).sort();
                if (levelNodes.get(k).getType()==1) {
                    cnue = false;
                    leafcnt++;
                    for (IndexElement ie : levelNodes.get(k).getElementList()) {
                        if (levelNodes.get(k).getType()==1) {
                            leafamt++;
                        }
                    }
                } else {
                    nodecnt++;
                    for (int i=0; i<levelNodes.get(k).getElementList().size(); i++) {
                        nodeamt++;
                        inNodes.add(this.list.get((Integer)levelNodes.get(k).getElementList().get(i).getElement()));
                    }
                    if (k==levelNodes.size()-1) {
                        lc = levelNodes.get(k).getLc();
                        if (lc>0) {
                            inNodes.add(this.list.get(lc));
                        }
                    }
                }
            }
            levelNodes = inNodes;
        }
        return "nframes: "+nodecnt+" nds: "+nodeamt+" lframes: "+leafcnt+" lfs: "+leafamt;
    }

    private void addElementList (final IndexElementList el) {
        this.list.add(el);
        final int ptr = this.list.size()-1;
        el.setPtr(ptr);
    }

    public synchronized Object getObjectByKey (final int id) {
        return getObjectByKey (new IndexElementKey(new Integer[]{id}));
    }

    public synchronized Object getObjectByKey (final long id) {
        return getObjectByKey (new IndexElementKey(new Long[]{id}));
    }

    public synchronized Object getObjectByKey (final String id) {
        return getObjectByKey (new IndexElementKey(new String[]{id}));
    }

    //for unique indexes
    public synchronized Object getObjectByKey (final IndexElementKey key) {
        boolean cnue = true;
        IndexElementList target = this.list.get(this.start);
        while (cnue) {
            if (target.getType()==1) { //leaf
                cnue = false;
            } else {
                final int cptr = target.getChildElementPtr(key);
                if (cptr>=0) {
                    target = list.get(cptr);
                } else {
                    target = list.get(target.getLc()); //get by last child
                }
            }
        }
        return target.getObjectByKey(key);
    }


    public synchronized List<Object> getObjectsByKey (final int obj) {
        return getObjectsByKey (new IndexElementKey(new Integer[]{obj}));
    }

    public synchronized List<Object> getObjectsByKey (final long obj) {
        return getObjectsByKey (new IndexElementKey(new Long[]{obj}));
    }

    //for non-unique indexes
    public synchronized List<Object> objectsByKey (final IndexElementKey key) {
        final ArrayList<Object> r = new ArrayList<Object>();
        boolean cnue = true;
        ArrayList<IndexElementList> targets = new ArrayList<IndexElementList>();
        targets.add(this.list.get(this.start));
        while (cnue) {
            final ArrayList<IndexElementList> ntargets = new ArrayList<IndexElementList>();
            for (IndexElementList target : targets) {
                if (target.getType()==1) { //leaf
                    r.addAll(target.getObjectsByKey(key));
                    cnue = false;
                } else {
                    final ArrayList<Integer> cptr = target.getChildElementsPtr(key);
                    if (cptr.size()>0) {
                        for (Integer i : cptr) {
                            ntargets.add(list.get(i));
                        }
                    }
                    if (target.getLc()>0) {
                        ntargets.add(list.get(target.getLc())); //get by last child
                    }
                }
            }
            targets = ntargets;
        }
        return r;
    }

    public synchronized Object getFirstObjectByKey (final int obj) {
        return getFirstObjectByKey (new IndexElementKey(new Integer[]{obj}));
    }

    //for non-unique indexes
    public synchronized Object getFirstObjectByKey (final IndexElementKey key) {
        boolean cnue = true;
        ArrayList<IndexElementList> targets = new ArrayList<IndexElementList>();
        targets.add(this.list.get(this.start));
        while (cnue) {
            final ArrayList<IndexElementList> ntargets = new ArrayList<IndexElementList>();
            for (IndexElementList target : targets) {
                if (target.getType()==1) { //leaf
                    final List<Object> lfs = target.getObjectsByKey(key);
                    if (lfs.size() > 0) {
                        return lfs.get(0);
                    }
                    cnue = false;
                } else {
                    final ArrayList<Integer> cptr = target.getChildElementsPtr(key);
                    if (cptr.size()>0) {
                        for (Integer i : cptr) {
                            ntargets.add(list.get(i));
                        }
                    }
                    if (target.getLc()>0) {
                        ntargets.add(list.get(target.getLc())); //get by last child
                    }
                }
            }
            targets = ntargets;
        }
        return null;
    }

    //find object(s) by key and update unique object (param)
    public synchronized void updateObjects (IndexElementKey key, Object o) {
        boolean cnue = true;
        ArrayList<IndexElementList> targets = new ArrayList<IndexElementList>();
        targets.add(this.list.get(this.start));
        while (cnue) {
            ArrayList<IndexElementList> ntargets = new ArrayList<IndexElementList>();
            for (IndexElementList target : targets) {
                if (target.getType()==1) { //leaf
                    target.updateObjects(key, o);
                    cnue = false;
                } else {
                    ArrayList<Integer> cptr = target.getChildElementsPtr(key);
                    if (cptr.size()>0) {
                        for (Integer i : cptr) {
                            ntargets.add(list.get(i));
                        }
                    }
                    if (target.getLc()>0) {
                        ntargets.add(list.get(target.getLc())); //get by last child
                    }
                }
            }
            targets = ntargets;
        }
    }

    //find object(s) by key and remove unique object (param)
    public synchronized void removeObjects (IndexElementKey key, Object o) {
        boolean cnue = true;
        ArrayList<IndexElementList> targets = new ArrayList<IndexElementList>();
        targets.add(this.list.get(this.start));
        while (cnue) {
            ArrayList<IndexElementList> ntargets = new ArrayList<IndexElementList>();
            for (IndexElementList target : targets) {
                if (target.getType()==1) { //leaf
                    target.removeObjects(key, o);
                    cnue = false;
                } else {
                    ArrayList<Integer> cptr = target.getChildElementsPtr(key);
                    if (cptr.size()>0) {
                        for (Integer i : cptr) {
                            ntargets.add(list.get(i));
                        }
                    }
                    if (target.getLc()>0) {
                        ntargets.add(list.get(target.getLc())); //get by last child
                    }
                }
            }
            targets = ntargets;
        }
    }

}
