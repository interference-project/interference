/**
 The MIT License (MIT)

 Copyright (c) 2010-2025 interference

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

package su.interference.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.core.*;
import su.interference.mgmt.*;
import su.interference.persistent.*;
import su.interference.persistent.Cursor;
import su.interference.sql.SQLCursor;
import su.interference.transport.HeartBeatProcess;
import su.interference.transport.TransportChannel;

import javax.persistence.Id;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class MCContainer {
    //todo -> Config
    public static String mmhost = "localhost";
    private final static Logger logger = LoggerFactory.getLogger(MCContainer.class);

    private static String getContent(String tableId, String sessionId, String pageId, String fileId) throws Exception {
        String result = "<table class=head border=0 cellpadding=5 cellspacing=1 width=100% height=100%>\n";
        result = result + "<tr><td width=10% height=50 align=left valign=center>"+getPageA1(sessionId, 1, "System")+"</td>";
        result = result + "<td width=10% height=50 align=left valign=center>"+getPageA1(sessionId, 2, "Tables")+"</td>";
        result = result + "<td width=10% height=50 align=left valign=center>"+getPageA1(sessionId, 3, "Indexes")+"</td>";
        result = result + "<td width=10% height=50 align=left valign=center>"+getPageA1(sessionId, 4, "Sessions")+"</td>";
        result = result + "<td width=10% height=50 align=left valign=center>"+getPageA1(sessionId, 5, "Transactions")+"</td>";
        result = result + "<td width=10% height=50 align=left valign=center>"+getPageA1(sessionId, 6, "SQL Queries")+"</td>";
        result = result + "<td width=10% height=50 align=left valign=center>"+getPageA1(sessionId, 7, "Frames")+"</td>";
        // result = result + "<td width=10% height=50 align=left valign=center>"+getPageA1(sessionId, 8, "Import")+"</td>";
        result = result + "<td width=50% height=50 align=left valign=center>&nbsp;</td></tr>";
        result = result + "<tr><td class=body colspan=8 width=100% height=100% align=left valign=top>"+getPageContent(tableId, pageId, fileId, sessionId)+"</td></tr></table>";
        return result;
    }

    private static String getPageA1(String sessionId, int pageId, String pageName) {
        return "<a class=hed href=\"?session_id="+sessionId+"&page_id="+pageId+"\">"+pageName+"</a>";
    }

    private static String getPageContent(String tableId, String pageId, String fileId, String sessionId) throws Exception {
        if (pageId != null) {
            try {
                int id = Integer.valueOf(pageId);
                int file = (fileId == null || "".equals(fileId)) ? 1 : Integer.valueOf(fileId);
                switch (id) {
                    case 1:
                        return getSystemPage(sessionId, id);
                    case 2:
                        return getTablesPage(sessionId, id);
                    case 3:
                        return getIndexesPage(sessionId, id);
                    case 4:
                        return getSessionsPage(sessionId, id);
                    case 5:
                        return getTransactionsPage(sessionId, id);
                    case 6:
                        return getSQLQueriesPage(sessionId, id);
                    case 7:
                        return getFramesPage(sessionId, id, file);
                    case 8:
                        return getUploadForm(sessionId, id, mmhost, Config.getConfig().MMPORT);
                    case 10:
                        if (tableId != null && tableId.matches("-?\\d+(\\.\\d+)?")) {
                            int tId = Integer.valueOf(tableId);
                            return getTablePage(sessionId, id, tId);
                        } else {
                            return "Table identifier incorrect: "+ tableId;
                        }
                }
            } catch (NumberFormatException e) {
                return getSystemPage(sessionId, 1);
            }
        }
        return getSystemPage(sessionId, 1);
    }

    private static String getButtonForm(Class c, Object o, MgmtContainer mgmtcnt, String sessionId, String type, String objectId, int pageId, String host, int port) throws Exception {
        String btn = mgmtcnt.getMgmtAction().name();
        String command = mgmtcnt.getMethod().getName();
        if (btn == null || btn.equals("")) {
            return "";
        }
        String name = btn;
        if (btn.startsWith("@")) {
            String methodName = btn.substring(1);
            if (!methodName.equals("")) {
                Method m = c.getDeclaredMethod(methodName, null);
                if (m.getReturnType().getSimpleName().equals("String")) {
                    name = (String) m.invoke(o, null);
                    if (name == null) {
                        return "";
                    }
                } else {
                    throw new RuntimeException();
                }
            }
        }
        String result = "<form method=\"POST\" action=\"http://"+host+":"+port+"\">\n" +
                "<input type=\"hidden\" name=\"session_id\" value=\""+sessionId+"\">\n" +
                "<input type=\"hidden\" name=\"page_id\" value=\""+pageId+"\">\n" +
                "<input type=\"hidden\" name=\"object_id\" value=\""+objectId+"\">\n" +
                "<input type=\"hidden\" name=\"type\" value=\""+type+"\">\n" +
                "<input type=\"hidden\" name=\"command\" value=\""+command+"\">\n" +
                "<input type=\"hidden\" name=\"param\" value=\""+name+"\">\n" +
                "<input type=\"submit\" name=\"sys_button\" value=\""+name+"\" onclick=\"javascript:this.disabled=true; this.form.submit()\">\n" +
                "</form>\n";
        return result;
    }

    // getUploadForm(sessionId, pageId, mmhost, Config.getConfig().MMPORT)
    private static String getUploadForm(String sessionId, int pageId, String host, int port) {
        String result = "<form method=\"POST\" action=\"http://"+host+":"+port+"\" enctype=\"multipart/form-data\">\n" +
                "<input type=\"hidden\" name=\"session_id\" value=\""+sessionId+"\">\n" +
                "<input type=\"hidden\" name=\"page_id\" value=\""+pageId+"\">\n" +
                "<input type=\"file\" name=\"upload\">\n" +
                "<input type=\"submit\" name=\"sys_button\" value=\"Upload\" onclick=\"javascript:this.disabled=true; this.form.submit()\">\n" +
                "</form>\n";
        return result;
    }

    private static String getSystemPage(String sessionId, int pageId) throws Exception {
        return getChannelsPage(sessionId, pageId) + "<br>" + getConfigPage(sessionId, pageId) + "<br>" + getDataFilesPage(sessionId, pageId) + "<br>" +getProcessesPage(sessionId, pageId);
    }

    private static String getChannelsPage(String sessionId, int pageId) throws Exception {
        List<TransportChannel> channels = HeartBeatProcess.getChannelsMCC().values().stream().collect(Collectors.toList());
        return getContentByMgmtObjects(channels, sessionId, pageId, mmhost, Config.getConfig().MMPORT);
    }

    private static String getConfigPage(String sessionId, int pageId) throws Exception {
        List<MgmtConfig> configs = Config.getConfig().getMgmtConfigParams();
        return getContentByMgmtObjects(configs, sessionId, pageId, mmhost, Config.getConfig().MMPORT);
    }

    private static String getDataFilesPage(String sessionId, int pageId) throws Exception {
        List datafiles = Arrays.asList(Instance.getInstance().getDataFiles());
        return getContentByMgmtObjects(datafiles, sessionId, pageId, mmhost, Config.getConfig().MMPORT);
    }

    private static String getProcessesPage(String sessionId, int pageId) throws Exception {
        List processes = Instance.getInstance().getProcesses();
        return getContentByMgmtObjects(processes, sessionId, pageId, mmhost, Config.getConfig().MMPORT);
    }

    private static String getTablesPage(String sessionId, int pageId) throws Exception {
        List tables = Instance.getInstance().getTables().stream().filter(t -> !t.isNoTran()).filter(t -> !t.isIndex()).collect(Collectors.toList());
        return getContentByMgmtObjects(tables, sessionId, pageId, mmhost, Config.getConfig().MMPORT);
    }

    private static String getIndexesPage(String sessionId, int pageId) throws Exception {
        List tables = Instance.getInstance().getTables().stream().filter(t -> !t.isNoTran()).filter(t -> t.isIndex()).collect(Collectors.toList());
        return getContentByMgmtObjects(tables, sessionId, pageId, mmhost, Config.getConfig().MMPORT);
    }

    private static String getSessionsPage(String sessionId, int pageId) throws Exception {
        List<Session> sessions = Instance.getInstance().getSessions();
        return getContentByMgmtObjects(sessions, sessionId, pageId, mmhost, Config.getConfig().MMPORT);
    }

    private static String getTransactionsPage(String sessionId, int pageId) throws Exception {
        List<Transaction> transactions = Instance.getInstance().getTransactions();
        return getContentByMgmtObjects(transactions, sessionId, pageId, mmhost, Config.getConfig().MMPORT);
    }

    private static String getSQLQueriesPage2(String sessionId, int pageId) throws Exception {
        List<Cursor> cursors = Instance.getInstance().getCursors();
        return getContentByMgmtObjects(cursors, sessionId, pageId, mmhost, Config.getConfig().MMPORT);
    }

    private static String getSQLQueriesPage(String sessionId, int pageId) throws Exception {
        List<SQLCursor> cursors = new ArrayList<>();
        for (Cursor c: Instance.getInstance().getCursors()) {
            if (c.getSqlStmt() != null) {
                cursors.addAll(c.getSqlStmt().getSQLCursors());
            }
        }
        return getContentByMgmtObjects(cursors, sessionId, pageId, mmhost, Config.getConfig().MMPORT);
    }

    private static String getTablePage(String sessionId, int pageId, int tableId) throws Exception {
        Session session = Instance.getInstance().getSession(sessionId);
        if (session != null) {
            Table table = Instance.getInstance().getTableById(tableId);
            if (table != null) {
                session.startTransaction();
                List<Object> objects = new ArrayList<>();
                Object o = table.poll(session);
                if (o != null) {
                    objects.add(o);
                    while (o != null) {
                        o = table.poll(session);
                        if (o != null) {
                            objects.add(o);
                        }
                    }
                }
                session.commit();
                return getContentByMgmtObjects(objects, sessionId, pageId, mmhost, Config.getConfig().MMPORT);
            }
        }
        return "No table data or session incorrect";
    }

    private static String getFramesPage(String sessionId, int pageId, int fileId) throws Exception {
        StringBuffer result = new StringBuffer();
        List<DataFile> datafiles = Arrays.asList(Instance.getInstance().getDataFiles());
        for (DataFile dataFile : datafiles) {
            if (dataFile.getFileId() == fileId) {
                result.append(dataFile.getFileName());
                result.append("&nbsp;");
            } else {
                result.append("<a class=sub href=\"?session_id=");
                result.append(sessionId);
                result.append("&page_id=");
                result.append(pageId);
                result.append("&file_id=");
                result.append(dataFile.getFileId());
                result.append("\">");
                result.append(dataFile.getFileName());
                result.append("</a>&nbsp;");
            }
        }
        result.append("<br><br>");
        int fsize = Instance.getInstance().getFrameSize();
        Map<Long, DataChunk> fmap = Instance.getInstance().getFramesMap();
        Map<Long, FrameData> frames = new HashMap<>();
        for (Map.Entry<Long, DataChunk> entry : fmap.entrySet()) {
            FrameData fd = (FrameData) entry.getValue().getEntity();
            if (fd.getFile() == fileId) {
                frames.put(entry.getKey(), fd);
            }
        }
        int size = frames.size();
        int i = 0;
        int cnt = 0;
        long prevId = fileId;
        boolean cnue = true;
        result.append("<table class=head border=0 cellpadding=5 cellspacing=1 width=100%>\n");
        while (cnue) {
            String cls = "";
            if (i%24 == 0) {
                result.append("<tr>");
            }
            long currId = prevId + fsize;
            FrameData frameData = frames.get(currId);
            prevId = currId;
            i++;
            if (frameData == null) {
                FreeFrame ff = Instance.getInstance().getFreeFrameById(currId);
                if (ff == null) {
                    result.append("<td class=fmiss width=6% height=50 align=left valign=top>");
                    result.append(currId);
                    result.append("</td>");
                } else {
                    result.append("<td class=ffree width=6% height=50 align=left valign=top>");
                    result.append(currId);
                    result.append("</td>");
                }
            } else {
                cnt++;
                if (frameData.isNoTran()) {
                    cls = "fsys";
                } else if (!frameData.isLocal()) {
                    cls = "fouter";
                } else {
                    if (frameData.isSynced()) {
                        if (frameData.isFrameBusy()) {
                            cls = "ftran";
                        } else {
                            cls = "fsync";
                        }
                    } else {
                        if (frameData.isFrameBusy()) {
                            cls = "ftranns";
                        } else {
                            cls = "fnosync";
                        }
                    }
                }
                result.append("<td class=");
                result.append(cls);
                result.append(" width=6% height=50 align=left valign=top>");
                result.append(frameData.getFrameId());
                result.append("<br>");
                result.append(frameData.getAllocId());
                result.append("<br>");
                result.append(frameData.getFrameUsed());
                result.append("</td>");
            }
            if (cnt == size) {
                cnue = false;
            }
        }
        while (i%24 < 23) {
            result.append("<td width=6% height=50 align=left valign=top>No frame</td>");
            i++;
        }
        if (i%24 == 23) {
            result.append("<td width=6% height=50 align=left valign=top>No frame</td>");
            result.append("</tr>");
        }
        result.append("</table>");
        return result.toString();
    }

    protected static String getMCContent(String tableId, String pageId, String fileId, Session s) throws Exception {
        return "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0 Transitional//EN\">\n" +
        "<html><head>\n" +
        "<META http-equiv=\"Content-Type\" content=\"text/html; charset=UTF8\">\n" +
        "<META http-equiv=\"EXPIRES\"      content=\"Wed, 07 Jul 2004 12:34:56 GMT\">\n" +
        "<META http-equiv=\"PRAGMA\"       content=\"NO-CACHE\">\n" +
        getCSS() +
        "<title>Interference management console</title></HEAD>\n" +
        "<body aLink=\"0000ff\" bgColor=\"efefef\" link=\"0000ff\" text=\"000000\" topMargin=\"0\" leftMargin=\"0\" rightMargin=\"0\" vLink=\"0000ff\">\n" +
        getContent(tableId, s.getSessionId(), pageId, fileId) +
        "</body></html>";
    }

    private static String getContentByMgmtObjects(List objects, String sessionId, int pageId, String host, int port) throws Exception {
        if (objects == null || objects.size() == 0) {
            return "";
        }
        Class c = objects.get(0).getClass();
        Annotation te = c.getAnnotation(TransEntity.class);
        if (te != null) {
            c = c.getSuperclass();
        }
        String type = c.getSimpleName();
        Annotation a = c.getAnnotation(MgmtClass.class);
        List<MgmtContainer> mgmtlist = new ArrayList();
        if (a != null) {
            Field[] fields = c.getDeclaredFields();
            Method[] methods = c.getDeclaredMethods();
            Field idField = null;
            for (Field f : fields) {
                Annotation id = f.getAnnotation(Id.class);
                if (id != null) {
                    idField = f;
                }
            }
            for (Field f : fields) {
                Annotation fa = f.getAnnotation(MgmtColumn.class);
                if (fa != null) {
                    Annotation la = f.getAnnotation(MgmtLink.class);
                    if (la == null) {
                        mgmtlist.add(new MgmtContainer((MgmtColumn) fa, null, null, f, null, c, idField));
                    } else {
                        mgmtlist.add(new MgmtContainer((MgmtColumn) fa, null, (MgmtLink) la, f, null, c, idField));
                    }
                }
            }
            for (Method m : methods) {
                Annotation ma = m.getAnnotation(MgmtAction.class);
                if (ma != null) {
                    mgmtlist.add(new MgmtContainer(null, (MgmtAction) ma, null, null, m, c, idField));
                }
            }
        }
        int size = mgmtlist.size();
        if (size > 0) {
            String result = "<table class=head border=0 cellpadding=5 cellspacing=1 width=100%>\n";
            result = result + "<tr>";
            for (MgmtContainer mgmtcnt : mgmtlist) {
                result = result + "<td class=head width="+mgmtcnt.getSize()+"% height=50 align=left valign=center>"+mgmtcnt.getHeader()+"</td>";
            }
            result = result + "</tr>";
            for (Object o : objects) {
                result = result + "<tr>";
                for (MgmtContainer mgmtcnt : mgmtlist) {
                    String objectId = mgmtcnt.getId(o);
                    String s = mgmtcnt.isCommand() ? getButtonForm(c, o, mgmtcnt, sessionId, type, objectId, pageId, host, port) : mgmtcnt.getValue(o, objectId, sessionId, pageId);
                    result = result + "<td class=body width="+mgmtcnt.getSize()+"% height=30 align=left valign=center>"+s+"</td>";
                }
                result = result + "</tr>";
            }
            result = result + "</table>\n";
            return result;
        }
        return "";
    }

    private static String getCSS() {
        return "<style type=text/css>\n" +
                ".title  { font-family:arial,Helvetica,Verdana; color:000066; font-size:8pt; font-weight:bold;}\n" +
                ".but  { font-family:arial,Helvetica,Verdana; color:000000; font-size:8pt}\n" +
                ".text  { font-family:arial,Helvetica,Verdana; color:000000; font-size:8pt}\n" +
                ".redtext  { font-family:arial,Helvetica,Verdana; color:ff0000; font-size:8pt; font-weight: 700}\n" +
                ".text2 { font-family:arial,Helvetica,Verdana; color:203060; font-size:12pt; font-weight: 700}\n" +
                "a:visited { font-family: Arial, Helvetica;  font-size:12pt; text-decoration:none; color:2040a0; }\n" +
                "a:hover { font-family: Arial, Helvetica; font-size:12pt; text-decoration:none; color:a0a0a0;}\n" +
                "a:link  { font-family: Arial, Helvetica; font-size:12pt; text-decoration:none; color:2040a0;}\n" +
                "a.hed:visited { font-family: Arial, Helvetica; font-size:8pt; text-decoration:none; font-weight: 500; color:ffffff; }\n" +
                "a.hed:hover { font-family: Arial, Helvetica; font-size:8pt; text-decoration:none; font-weight: 500; color:ffffff;}\n" +
                "a.hed:link  { font-family: Arial, Helvetica; font-size:8pt; text-decoration:none; font-weight: 500; color:ffffff;}\n" +
                "a.sub:link { font-family: Arial, Helvetica; font-size:8pt; text-decoration:underline; color:000070}\n" +
                "a.sub:visited { font-family: Arial, Helvetica; font-size:8pt; text-decoration:underline; color:000070; }\n" +
                "a.sub:hover { font-family: Arial, Helvetica; font-size:8pt; text-decoration:underline; color:0040a0;}\n" +
                "a.nav:link { font-family: verdana,arial, Helvetica, sans-serif; font-size:12pt; font-weight: 500; text-decoration:none; color: ffffff; }\n" +
                "a.nav:visited { font-family: verdana,arial, Helvetica, sans-serif; font-size:12pt; font-weight: 500; text-decoration:none; color: ffffff; }\n" +
                "a.nav:hover { font-family: verdana,arial, Helvetica, sans-serif; font-size:12pt; font-weight: 500; text-decoration:none; color: 0090ff; }\n" +
                "a.nav2:link { font-family: verdana,arial, Helvetica, sans-serif; font-size:8pt; font-weight: 500; text-decoration:none; color: ffffff; }\n" +
                "a.nav2:visited { font-family: verdana,arial, Helvetica, sans-serif; font-size:8pt; font-weight: 500; text-decoration:none; color: ffffff; }\n" +
                "a.nav2:hover { font-family: verdana,arial, Helvetica, sans-serif; font-size:8pt; font-weight: 500; text-decoration:none; color: a000a0; }\n" +
                "a.hnav:link { font-family: verdana,arial, Helvetica, sans-serif; font-size:7pt; font-weight: 600; text-decoration:none; color: ffffff; }\n" +
                "a.hnav:visited { font-family: verdana,arial, Helvetica, sans-serif; font-size:7pt; font-weight: 600; text-decoration:none; color: ffffff; }\n" +
                "a.hnav:hover { font-family: verdana,arial, Helvetica, sans-serif; font-size:7pt; font-weight: 600; text-decoration:none; color: ff0000; }\n" +
                "h1 {font-family: MS Sans Serif; font-size:40pt; color:ffffff;font-weight: 700}\n" +
                "h2 {font-family: Arial; font-size:12pt; color:ffffff;font-weight: 500}\n" +
                "h3 {font-family: MS Sans Serif; font-size:20pt; color:001020;font-weight: 700}\n" +
                "h4 {font-family: MS Sans Serif; font-size:8pt; color:ffffff;font-weight: 500}\n" +
                "h5 {font-family: MS Sans Serif; font-size:7pt; color:002040;font-weight: 500}\n" +
                "h6 {font-family: Arial;  font-size:7pt; color:002040;font-weight: 600}\n" +
                "p  {font-family: Arial,Helvetica,Verdana; font-size:12pt; font-weight: 500; color:001020}\n" +
                "font {  font-family:arial,Helvetica,Verdana; ; font-size:8pt;}\n" +
                ".a{font:12px MS Sans Serif;color:203060;margin-left: 30 px; text-indent: -25 px}\n" +
                ".b{font:12px Arial;color:black;margin-left: 67 px; text-indent: -62 px}\n" +
                ".head   {font-family: Courier; font-size:11pt; color:101020; background-color:808090; font-weight: 500}\n" +
                ".hd     {font-family: Arial; font-size:11pt; color:e0e0f0; font-weight: 500}\n" +
                ".headr  {font-family: Arial, Helvetica; font-size:8pt; color: #ff0000; background-color: #88dfff; }\n" +
                ".menu   {font-family: Arial; font-size:12pt; background-color:303060}\n" +
                ".menu2  {font-family: Arial; font-size:8pt; background-color:303060}\n" +
                ".tmenu  {font-family: Arial; font-size:8pt; color:202040; background-color:a0a0b0; }\n" +
                ".flat   {background-color: #e0e0e0; font-size:8pt; font-family: Courier New; color:#002020; text-align: left; border-style: solid; border-width: 1px; border-color:#606070; }\n" +
                ".flatbut {background-color: #406060; font-size:8pt; font-family: Arial; color:#ffffff; text-align: left; border-style: solid; border-width: 1px; border-color:404040; }\n" +
                ".phead   {font-family:arial,Helvetica,Verdana; font-size:9pt; color: #002030; font-weight: 700}\n" +
                "td {font-family: Arial; font-size:8pt; color: #000000; }\n" +
                "td.even  {font-family: Arial; color: #002040; background-color:f0f0f0}\n" +
                "td.head {font-family: Arial; font-size:11pt; color: #002040; background-color:afbfd0}\n" +
                "td.odd  {font-family: Arial; background-color: #cfefd0; }\n" +
                "td.body {font-family: Arial,Helvetica,Verdana; font-size:8pt; color: #001020; background-color:e8e8f8; font-weight: 500}\n" +
                "td.fsync {font-family: Arial,Helvetica,Verdana; font-size:9pt; color: #001020; background-color:b8f8b8; font-weight: 500}\n" +
                "td.fnosync {font-family: Arial,Helvetica,Verdana; font-size:9pt; color: #001020; background-color:b8b880; font-weight: 500}\n" +
                "td.ftran {font-family: Arial,Helvetica,Verdana; font-size:9pt; color: #001020; background-color:b88840; font-weight: 500}\n" +
                "td.ftranns {font-family: Arial,Helvetica,Verdana; font-size:9pt; color: #001020; background-color:d84840; font-weight: 500}\n" +
                "td.fouter {font-family: Arial,Helvetica,Verdana; font-size:9pt; color: #001020; background-color:2848d8; font-weight: 500}\n" +
                "td.fsys {font-family: Arial,Helvetica,Verdana; font-size:9pt; color: #001020; background-color:28a8d8; font-weight: 500}\n" +
                "td.fmiss {font-family: Arial,Helvetica,Verdana; font-size:9pt; color: #001020; background-color:d80000; font-weight: 500}\n" +
                "td.ffree {font-family: Arial,Helvetica,Verdana; font-size:9pt; color: #001020; background-color:e8e8e8; font-weight: 500}\n" +
                "td.phead {font-family: Arial,Helvetica,Verdana; font-size:9pt; color:002040; font-weight: 700}\n" +
                "td.back {font-family: Arial; color: #002040; background-color:c0b0b0}\n" +
                "table.head { font-size:10pt; background-color: 003060; }\n" +
                "table.headl { font-size:8pt; background-image:url(img/headl.jpg); background-color: 003060; }\n" +
                "img.bl {width: 10; height: 20; border: 0}\n" +
                "img.ico {width: 16; height: 16; border: 0}\n" +
                "img.but {width: 16; height: 16; border: 0; cursor: hand}\n" +
                "img.dlmt {width: 445; height: 10; border: 0}\n" +
                "img.dlmt1 {width: 125; height: 10; border: 0}\n" +
                "ul { font-family:arial,Helvetica,Verdana; font-size:11pt; color:002040}\n" +
                "li { font-family:arial,Helvetica,Verdana; font-size:12pt; color:002040; padding:6}\n" +
                "li.err  { font-family:arial,Helvetica,Verdana; font-size:8pt; color:ff0000}\n" +
                "li.mesg { font-family:arial,Helvetica,Verdana; font-size:8pt; color:002040}\n" +
                "dl { font-family:arial,Helvetica,Verdana; font-size:8pt}\n" +
                "dt { font-family:arial,Helvetica,Verdana; font-size:8pt}\n" +
                "dd { font-family:arial,Helvetica,Verdana; font-size:8pt; font-style: italic}\n" +
                "pre { color: #001020; background-color:e0e0f0}\n" +
                "code {font-size:14pt; color:101020; font-weight: 500}\n" +
                "</style>\n";
    }
}
