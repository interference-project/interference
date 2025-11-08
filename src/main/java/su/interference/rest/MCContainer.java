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
import su.interference.core.Config;
import su.interference.core.Instance;
import su.interference.persistent.Node;
import su.interference.persistent.Session;
import su.interference.transport.HeartBeatProcess;
import su.interference.transport.TransportChannel;

import java.util.Map;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class MCContainer {
    //todo -> Config
    public static String mmhost = "localhost";
    private final static Logger logger = LoggerFactory.getLogger(MCContainer.class);

    private static String getContent(String sessionId, String pageId) throws Exception {
        String result = "<table class=head border=0 cellpadding=5 cellspacing=1 width=100% height=100%>\n";
        result = result + "<tr><td width=10% height=50 align=left valign=center>"+getPageA1(sessionId, 1, "System")+"</td>";
        result = result + "<td width=10% height=50 align=left valign=center>"+getPageA1(sessionId, 2, "Tables")+"</td>";
        result = result + "<td width=10% height=50 align=left valign=center>"+getPageA1(sessionId, 3, "Sessions")+"</td>";
        result = result + "<td width=10% height=50 align=left valign=center>"+getPageA1(sessionId, 4, "Transactions")+"</td>";
        result = result + "<td width=10% height=50 align=left valign=center>"+getPageA1(sessionId, 5, "SQL Queries")+"</td>";
        result = result + "<td width=50% height=50 align=left valign=center>&nbsp;</td></tr>";
        result = result + "<tr><td class=body colspan=6 width=100% height=100% align=left valign=top>"+getPageContent(pageId, sessionId)+"</td></tr></table>";
        return result;
    }

    private static String getPageA1(String sessionId, int pageId, String pageName) {
        return "<a class=hed href=\"?session_id="+sessionId+"&page_id="+pageId+"\">"+pageName+"</a>";
    }

    private static String getPageA2(String sessionId, int pageId, String pageName) {
        return "<a href=\"?session_id="+sessionId+"&page_id="+pageId+"\">"+pageName+"</a>";
    }

    private static String getPageContent(String pageId, String sessionId) throws Exception {
        if (pageId != null) {
            try {
                int id = Integer.valueOf(pageId);
                switch (id) {
                    case 1:
                        return getSystemPage(sessionId, id);
                    case 2:
                        return getTablesPage();
                    case 3:
                        return getSessionsPage();
                    case 4:
                        return getTransactionsPage();
                    case 5:
                        return getSQLQueriesPage();
                }
            } catch (NumberFormatException e) {
                return "Wrong page idetifier";
            }
        }
        return "No page defined";
    }

    private static String getSystemPage(String sessionId, int pageId) throws Exception {
        Map<Integer, TransportChannel> channels = HeartBeatProcess.getChannels();
        String result = "<table class=head border=0 cellpadding=5 cellspacing=1 width=100%>\n";
        result = result + "<tr><td bgcolor=#ffffff width=10% height=50 align=left valign=top>"+Instance.getInstance().getLocalNodeId()+"</td>";
        result = result + "<td bgcolor=#ffffff width=10% height=50 align=left valign=center>"+mmhost+"</td>";
        result = result + "<td bgcolor=#ffffff width=10% height=50 align=left valign=center>"+Config.getConfig().RMPORT+"</td>";
        result = result + "<td bgcolor="+Instance.getInstance().getSystemStateCellColor(Instance.getInstance().getLocalNodeId())+" width=10% height=50 align=left valign=center>"+Instance.getInstance().getSystemStateString(Instance.getInstance().getLocalNodeId())+"</td>";
        result = result + "<td bgcolor=#ffffff width=10% height=50 align=left valign=center>"+getStartupButtonForm(sessionId, Instance.getInstance().getLocalNodeId(), pageId, mmhost, Config.getConfig().MMPORT)+"</td>";
        result = result + "<td bgcolor=#ffffff width=10% height=50 align=left valign=center>LOCAL</td></tr>";
        for (Map.Entry<Integer, TransportChannel> entry : channels.entrySet()) {
            result = result + "<tr><td bgcolor=#ffffff width=10% height=50 align=left valign=center>"+entry.getKey()+"</td>";
            result = result + "<td bgcolor=#ffffff width=10% height=50 align=left valign=center>"+entry.getValue().getHost()+"</td>";
            result = result + "<td bgcolor=#ffffff width=10% height=50 align=left valign=center>"+entry.getValue().getPort()+"</td>";
            result = result + "<td bgcolor="+Instance.getInstance().getSystemStateCellColor(entry.getKey())+" width=10% height=50 align=left valign=center>"+Instance.getInstance().getSystemStateString(entry.getKey())+"</td>";
            result = result + "<td bgcolor=#ffffff width=10% height=50 align=left valign=center>"+getStartupButtonForm(sessionId, entry.getKey(), pageId, mmhost, Config.getConfig().MMPORT)+"</td>";
            result = result + "<td bgcolor=#ffffff width=10% height=50 align=left valign=center>REMOTE</td></tr>";
        }
        result = result + "</table>\n";

        return result;
    }

    private static String getStartupButtonForm(String sessionId, int nodeId, int pageId, String host, int port) throws Exception {
        String btn = Instance.getInstance().getBtnStringByState(nodeId);
        String result = btn == null ? "" : "<form method=\"POST\" action=\"http://"+host+":"+port+"\">\n" +
                "<input type=\"hidden\" name=\"session_id\" value=\""+sessionId+"\">\n" +
                "<input type=\"hidden\" name=\"page_id\" value=\""+pageId+"\">\n" +
                "<input type=\"hidden\" name=\"node_id\" value=\""+nodeId+"\">\n" +
                "<input type=\"hidden\" name=\"command\" value=\""+btn+"\">\n" +
                "<input type=\"submit\" name=\"sys_button\" value=\""+btn+"\">\n" +
                "</form>\n";
        return result;
    }

    private static String getTablesPage() {
        return "TABLES";
    }

    private static String getSessionsPage() {
        return "SESSIONS";
    }

    private static String getTransactionsPage() {
        return "TRANSACTIONS";
    }

    private static String getSQLQueriesPage() {
        return "SQL QUERIES";
    }

    protected static String getMCContent(String pageId, Session s) throws Exception {
        return "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0 Transitional//EN\">\n" +
        "<html><head>\n" +
        "<META http-equiv=\"Content-Type\" content=\"text/html; charset=UTF8\">\n" +
        "<META http-equiv=\"EXPIRES\"      content=\"Wed, 07 Jul 2004 12:34:56 GMT\">\n" +
        "<META http-equiv=\"PRAGMA\"       content=\"NO-CACHE\">\n" +
        getCSS() +
        "<title>Interference management console</title></HEAD>\n" +
        "<body aLink=\"0000ff\" bgColor=\"efefef\" link=\"0000ff\" text=\"000000\" topMargin=\"0\" leftMargin=\"0\" rightMargin=\"0\" vLink=\"0000ff\">\n" +
        getContent(s.getSessionId(), pageId) +
        "</body></html>";
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
                "a.ora:link { font-family: verdana,arial, Helvetica, sans-serif; font-size:8pt; font-weight: 500; text-decoration:none; color: ffffff; }\n" +
                "a.ora:visited { Font-family: verdana,arial, Helvetica, sans-serif; font-size:8pt; font-weight: 500; text-decoration:none; color: ffffff; }\n" +
                "a.ora:hover { font-family: verdana,arial, Helvetica, sans-serif; font-size:8pt; font-weight: 500; text-decoration:none; color: ffffff; }\n" +
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
                "td.head {font-family: Arial; font-size:12pt; color: #002040; background-color:dfcfc0}\n" +
                "td.odd  {font-family: Arial; background-color: #cfefd0; }\n" +
                "td.body {font-family:arial,Helvetica,Verdana; font-size:12pt; color: #001020; background-color:e8e8e8; font-weight: 500}\n" +
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
