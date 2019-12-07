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

import su.interference.sql.ResultSet;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class DataSet {
    private Object[]  ds;
    private ResultSet table;
    private String    message;

    public DataSet(Object[] ds) {
        this.ds = ds;
    }

    public DataSet(Object[] ds, String message) {
        this.ds      = ds;
        this.message = message;
    }

    public DataSet(Object[] ds, ResultSet table, String message) {
        this.ds      = ds;
        this.table   = table;
        this.message = message;
    }

    public Object[] getDataSet() {
        return ds;
    }

    public void setDataSet(Object[] ds) {
        this.ds = ds;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public ResultSet getTable() {
        return table;
    }

    public void setTable(ResultSet table) {
        this.table = table;
    }
}
