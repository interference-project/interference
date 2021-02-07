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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class Config {

    private static final String P_LOCAL_NODE_ID = "local.node.id";
    private static final String P_DB_PATH = "db.path";
    private static final String P_JOURNAL_PATH = "journal.path";
    private static final String P_CLUSTER_NODES = "cluster.nodes";
    private static final String P_REGISTER_CLASSES = "auto.register.classes";
    private static final String P_MMPORT = "mmport";
    private static final String P_RMPORT = "rmport";
    private static final String P_RMHOST_START="rmhost.start";
    private static final String P_RMHOST_RANGE="rmhost.range";
    private static final String P_RMPORT_START="rmport.start";
    private static final String P_RMPORT_RANGE="rmport.range";
    private static final String P_FRAMESIZE = "frame.size";
    private static final String P_FRAMESIZE2 = "frame.size.ix";
    private static final String P_FILES_AMOUNT = "files.amount";
    private static final String P_DISKIO_MODE = "diskio.mode";
    private static final String P_SYNC_LOCK_ENABLE = "sync.lock.enable";
    private static final String P_SYNC_PERIOD = "sync.period";
    private static final String P_RETRIEVE_QUEUE_SIZE = "retrieve.queue.size";
    private static final String P_CODEPAGE = "codepage";
    private static final String P_DATEFORMAT = "dateformat";
    private static final int MAX_NODE_ID = 64;
    private static final int MAX_FILES_AMOUNT = 32;

    private static final int LOCAL_NODE_ID_DEFAULT=1;
    private static final String DB_PATH_DEFAULT="";
    private static final String JOURNAL_PATH_DEFAULT="";
    private static final String[] CLUSTER_NODES_DEFAULT=new String[]{};
    private static final String[] REGISTER_CLASSES_DEFAULT=new String[]{};
    private static final int MMPORT_DEFAULT=8086;
    private static final int RMPORT_DEFAULT=8050;
    private static final String RMHOST_START_DEFAULT="127.0.0.1";
    private static final int RMHOST_RANGE_DEFAULT=1;
    private static final int RMPORT_START_DEFAULT=8050;
    private static final int RMPORT_RANGE_DEFAULT=1;
    private static final int FRAMESIZE_DEFAULT=4096;
    private static final int FRAMESIZE2_DEFAULT=4096;
    private static final int FILES_AMOUNT_DEFAULT=4;
    private static final String DISKIO_MODE_DEFAULT="rws";
    private static final boolean SYNC_LOCK_ENABLE_DEFAULT=false;
    private static final int SYNC_PERIOD_DEFAULT=2000;
    private static final int RETRIEVE_QUEUE_SIZE_DEFAULT=10000;
    private static final String CODEPAGE_DEFAULT="UTF-8";
    private static final String DATEFORMAT_DEFAULT="dd.MM.yyyy";

    public final int LOCAL_NODE_ID;
    public final String DB_PATH;
    public final String JOURNAL_PATH;
    public final String[] CLUSTER_NODES;
    public final String[] REGISTER_CLASSES;
    public final int MMPORT;
    public final int RMPORT;
    public final String RMHOST_START;
    public final int RMHOST_RANGE;
    public final int RMPORT_START;
    public final int RMPORT_RANGE;
    public final int FRAMESIZE;
    public final int FRAMESIZE2;
    public final int FILES_AMOUNT;
    public final String DISKIO_MODE;
    public final boolean SYNC_LOCK_ENABLE;
    public final int SYNC_PERIOD;
    public final int RETRIEVE_QUEUE_SIZE;
    public final int RETRIEVE_THREADS_AMOUNT = 8;
    public final String CODEPAGE;
    public final String DATEFORMAT;
    public final int TEST_DISTRIBUTE_MODE = 1;
    public final int CHECK_AVAIL_FRAME_TIMEOUT = 3000;
    // transport
    public final int REMOTE_SYNC_TIMEOUT = 60000;
    public final int READ_BUFFER_SIZE = 33554432;
    public final int WRITE_BUFFER_SIZE = 33554432;
    // cleanup
    public final int TRANS_CLEANUP_TIMEOUT = 5000;
    public final int CLEANUP_ENABLE = 1;
    public final int CLEANUP_TIMEOUT = 3000;
    public final int CLEANUP_PROTECTION_THR = 1000;
    public final int IX_CLEANUP_PROTECTION_THR = 2000;
    public final int HEAP_USE_THR_DATA = 60;
    public final int HEAP_USE_THR_INDX = 60;
    public final int HEAP_USE_THR_TEMP = 40;
    public final int HEAP_USE_THR_UNDO = 50;


    private final Properties p;

    public static final String[] cps = new String[]{"Cp858","Cp437","Cp775","Cp850","Cp852","Cp855","Cp857","Cp862","Cp866","ISO8859_1","ISO8859_2","ISO8859_4","ISO8859_5","ISO8859_7",
                         "ISO8859_9","ISO8859_13","ISO8859_15","KOI8_R","KOI8_U","ASCII","UTF8","UTF-16","UnicodeBigUnmarked","UnicodeLittleUnmarked","UTF_32","UTF_32BE","UTF_32LE",
                         "UTF_32BE_BOM","UTF_32LE_BOM","Cp1250","Cp1251","Cp1252","Cp1253","Cp1254","Cp1257","UnicodeBig","Cp737","Cp874","UnicodeLittle"};

    public static final String[] dfs = new String[]{"dd.MM.yyyy", "dd.MM.yyyy HH:mm", "dd.MM.yyyy HH:mm:ss",
                                                    "MM.dd.yyyy", "MM.dd.yyyy HH:mm", "MM.dd.yyyy HH:mm:ss",
                                                    "yyyy.MM.dd", "yyyy.MM.dd HH:mm", "yyyy.MM.dd HH:mm:ss"};

    public static final int[] bss = new int[]{4096, 8192, 16384, 32768, 65536, 131072, 262044, 524288};
    private final static Logger logger = LoggerFactory.getLogger(Config.class);
    private static Config config;

    public static synchronized Config getConfig() {
        if (config==null) {
            config = new Config();
        }
        return config;
    }

    private Config() {
        p = new Properties();
        boolean ok = false;
        try {
            String cname = System.getProperty("su.interference.config");
            if (cname!=null) {
                p.load(new FileInputStream(new File("config/"+cname)));
            } else {
                p.load(new FileInputStream(new File("config/properties")));
            }
            ok = true;
        } catch (IOException e) {
        }
        if (ok) {
            DB_PATH = validatePath(p.getProperty(P_DB_PATH));
            JOURNAL_PATH = validatePath(p.getProperty(P_JOURNAL_PATH));
            LOCAL_NODE_ID = validateNodeId(p.getProperty(P_LOCAL_NODE_ID));
            CLUSTER_NODES = validateClusterNodes(p.getProperty(P_CLUSTER_NODES));
            REGISTER_CLASSES = validateRegisterClasses(p.getProperty(P_REGISTER_CLASSES));
            MMPORT = validateMMPort(p.getProperty(P_MMPORT));
            RMPORT = validateRMPort(p.getProperty(P_RMPORT));
            RMHOST_START = validateRMHost(p.getProperty(P_RMHOST_START));
            RMHOST_RANGE = validateHostRange(p.getProperty(P_RMHOST_RANGE));
            RMPORT_START = validateRMPort(p.getProperty(P_RMPORT_START));
            RMPORT_RANGE = validatePortRange(p.getProperty(P_RMPORT_RANGE));
            FRAMESIZE = validateFrameSize(p.getProperty(P_FRAMESIZE));
            FRAMESIZE2 = validateFrameSize(p.getProperty(P_FRAMESIZE2));
            CODEPAGE = validateCodePage(p.getProperty(P_CODEPAGE));
            DATEFORMAT = validateDateFormat(p.getProperty(P_DATEFORMAT));
            FILES_AMOUNT = validateFilesAmount(p.getProperty(P_FILES_AMOUNT));
            DISKIO_MODE = validateDiskioMode(p.getProperty(P_DISKIO_MODE));
            SYNC_LOCK_ENABLE = validateSyncLock(p.getProperty(P_SYNC_LOCK_ENABLE));
            SYNC_PERIOD = validateSyncPeriod(p.getProperty(P_SYNC_PERIOD));
            RETRIEVE_QUEUE_SIZE = validateQueueSize(p.getProperty(P_RETRIEVE_QUEUE_SIZE));
        } else {
            DB_PATH = DB_PATH_DEFAULT;
            JOURNAL_PATH = JOURNAL_PATH_DEFAULT;
            LOCAL_NODE_ID = LOCAL_NODE_ID_DEFAULT;
            CLUSTER_NODES = CLUSTER_NODES_DEFAULT;
            REGISTER_CLASSES = REGISTER_CLASSES_DEFAULT;
            MMPORT = MMPORT_DEFAULT;
            RMPORT = RMPORT_DEFAULT;
            RMHOST_START = RMHOST_START_DEFAULT;
            RMHOST_RANGE = RMHOST_RANGE_DEFAULT;
            RMPORT_START = RMPORT_START_DEFAULT;
            RMPORT_RANGE = RMPORT_RANGE_DEFAULT;
            FRAMESIZE = FRAMESIZE_DEFAULT;
            FRAMESIZE2 = FRAMESIZE2_DEFAULT;
            CODEPAGE = CODEPAGE_DEFAULT;
            DATEFORMAT = DATEFORMAT_DEFAULT;
            FILES_AMOUNT = FILES_AMOUNT_DEFAULT;
            DISKIO_MODE = DISKIO_MODE_DEFAULT;
            SYNC_LOCK_ENABLE = SYNC_LOCK_ENABLE_DEFAULT;
            SYNC_PERIOD = SYNC_PERIOD_DEFAULT;
            RETRIEVE_QUEUE_SIZE = RETRIEVE_QUEUE_SIZE_DEFAULT;
        }
        System.setProperty("com.sun.management.jmxremote.port","8111");
        System.setProperty("com.sun.management.jmxremote.authenticate","false");
        System.setProperty("com.sun.management.jmxremote.ssl","false");
    }

    private static String[] validateClusterNodes(String value) {
        ArrayList<String> l = new ArrayList<String>();
        if (value!=null) {
            StringTokenizer st = new StringTokenizer(value, ",");
            while (st.hasMoreTokens()) {
                l.add(st.nextToken());
            }
        }
        return l.toArray(new String[]{});
    }

    private static String[] validateRegisterClasses(String value) {
        ArrayList<String> l = new ArrayList<String>();
        if (value!=null) {
            StringTokenizer st = new StringTokenizer(value, ",");
            while (st.hasMoreTokens()) {
                l.add(st.nextToken());
            }
        }
        return l.toArray(new String[]{});
    }

    private static boolean validateSyncLock(String value) {
        if ("true".equals(value)) {
            return true;
        }
        if ("false".equals(value)) {
            return false;
        }
        logger.error("sync.lock.enable value is not valid - use default");
        return SYNC_LOCK_ENABLE_DEFAULT;
    }

    private static int validateNodeId(String value) {
        try {
            final int id = Integer.valueOf(value);
            if (id >= 1 && id <= MAX_NODE_ID) {
                return id;
            }
        } catch(NumberFormatException e) {
            logger.error("node id " + value + " is not valid - use default");
        }
        return LOCAL_NODE_ID_DEFAULT;
    }

    private static String validatePath(String path) {
        if (path==null||path.equals("")) {
            logger.error("path is not valid - use default");
        }
        return path;
    }

    private static int validateFrameSize(String value) {
        try {
            final int bsize = Integer.valueOf(value);
            for (int bs : bss) {
                if (bs == bsize) {
                    return bsize;
                }
            }
        } catch (NumberFormatException e) {
            logger.error("frame size " + value + " is not valid - use default");
        }
        return FRAMESIZE_DEFAULT;
    }

    private static int validateMMPort(String value) {
        try {
            final int port = Integer.valueOf(value);
            if (port >= 1 && port <= 65536) {
                return port;
            }
        } catch (NumberFormatException e) {
            logger.error("management console port " + value + " is not valid - use default");
        }
        return MMPORT_DEFAULT;
    }

    private static int validateRMPort(String value) {
        try {
            final int port = Integer.valueOf(value);
            if (port >= 1 && port <= 65536) {
                return port;
            }
        } catch (NumberFormatException e) {
            logger.error("remote port " + value + " is not valid - use default");
        }
        return RMPORT_DEFAULT;
    }

    private static String validateCodePage(String cp) {
        for (String c : cps) {
            if (c.equals(cp)) {
                return cp;
            }
        }
        logger.error("codepage '"+cp+"' is not valid - use default");
        return CODEPAGE_DEFAULT;
    }

    private static String validateDateFormat(String df) {
        for (String d : dfs) {
            if (d.equals(df)) {
                return df;
            }
        }
        logger.error("dateformat '"+df+"' is not valid - use default");
        return DATEFORMAT_DEFAULT;
    }

    private static int validateFilesAmount(String value) {
        try {
            final int id = Integer.valueOf(value);
            if (id >= 1 && id <= MAX_FILES_AMOUNT) {
                return id;
            }
        } catch(NumberFormatException e) {
            logger.error("files amount " + value + " is not valid - use default");
        }
        return FILES_AMOUNT_DEFAULT;
    }

    private static String validateDiskioMode(String mode) {
        if (mode==null||mode.equals("")) {
            logger.error("diskio mode is not valid - use default");
            return DISKIO_MODE_DEFAULT;
        }
        if (mode.toUpperCase().equals("SYNC")) {
            return "rws";
        } else if (mode.toUpperCase().equals("ASYNC")) {
            return "rw";
        } else {
            logger.error("diskio mode is not valid - use default");
            return DISKIO_MODE_DEFAULT;
        }
    }

    private static int validateSyncPeriod(String value) {
        try {
            int p = Integer.valueOf(value);
            if (p > 9 && p < 60001) {
                return p;
            }
        } catch(NumberFormatException e) {
            logger.error("sync period value is not valid - use default value");
        }
        return SYNC_PERIOD_DEFAULT;
    }

    private static int validateQueueSize(String value) {
        try {
            int p = Integer.valueOf(value);
            if (p > 0 && p < 100000000) {
                return p;
            }
        } catch(NumberFormatException e) {
            logger.error("sync period value is not valid - use default value");
        }
        return SYNC_PERIOD_DEFAULT;
    }

    private static String validateRMHost(String value) {
        if (value==null||value.equals("")) {
            logger.error("host start is not valid - use default");
            return RMHOST_START_DEFAULT;
        }
        if (value.equals("localhost")) {
            return "localhost";
        } else {
            StringTokenizer st = new StringTokenizer(value, ".");
            try {
                while (st.hasMoreTokens()) {
                    int v = Integer.valueOf(st.nextToken());
                }
                return value;
            } catch(NumberFormatException e) {
                logger.error("host start is not valid - use default");
                return RMHOST_START_DEFAULT;
            }
        }
    }

    private static int validateHostRange(String value) {
        try {
            int p = Integer.valueOf(value);
            if (p > 0 && p < 256) {
                return p;
            }
        } catch(NumberFormatException e) {
            logger.error("host range value is not valid - use default value");
        }
        return RMHOST_RANGE_DEFAULT;
    }

    private static int validatePortRange(String value) {
        try {
            int p = Integer.valueOf(value);
            if (p > 0 && p < 1000) {
                return p;
            }
        } catch(NumberFormatException e) {
            logger.error("port range value is not valid - use default value");
        }
        return RMPORT_RANGE_DEFAULT;
    }

    public String[] getRMHostRange() {
        if (RMHOST_START.equals("localhost")) {
            return new String[]{"localhost"};
        } else if (RMHOST_START.equals("127.0.0.1")) {
                return new String[]{"127.0.0.1"};
        } else {
            StringTokenizer st = new StringTokenizer(RMHOST_START, ".");
            int v = 0;
            int c = 0;
            String s = "";
            while (st.hasMoreTokens()) {
                v = Integer.valueOf(st.nextToken());
                if (c<3) s = s + v + ".";
                c++;
            }
            String[] r = new String[RMHOST_RANGE];
            for (int i=0; i<RMHOST_RANGE; i++) {
                r[i] = s + (v+i);
            }
            return r;
        }
    }

    public int[] getRMPortRange() {
        int[] r = new int[RMPORT_RANGE];
        for (int i=0; i<RMPORT_RANGE; i++) {
            r[i] = RMPORT_START+i;
        }
        return r;
    }

}
