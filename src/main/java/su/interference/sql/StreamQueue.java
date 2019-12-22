package su.interference.sql;

import su.interference.core.Chunk;
import su.interference.core.DataChunk;
import su.interference.exception.InternalException;
import su.interference.persistent.Session;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.PriorityBlockingQueue;

public class StreamQueue implements ResultSet {
    //private final PriorityBlockingQueue<Comparable> q = new PriorityBlockingQueue();
    private final ConcurrentLinkedQueue<Object> q = new ConcurrentLinkedQueue();

    public DataChunk persist(Object o, Session s) throws Exception {
        q.add(o);
        return null;
    }

    public Object poll() {
        return q.poll();
    }

    public List<Chunk> getAll(Session s) throws Exception {
        return null;
    }

    public ArrayList<Object> getAll(Session s, int ptr) throws Exception {
        return null;
    }

    public int getObjectId() {
        return 0;
    }

    public boolean isIndex() throws ClassNotFoundException, MalformedURLException {
        return false;
    }

    public Class getTableClass() throws ClassNotFoundException, MalformedURLException {
        return null;
    }

    public java.lang.reflect.Field[] getFields() throws ClassNotFoundException, InternalException, MalformedURLException {
        return null;
    }

    public void deallocate(Session s) throws Exception {

    }
}
