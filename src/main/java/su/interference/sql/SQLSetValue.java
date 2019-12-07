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

import su.interference.sqlexception.InvalidColumnDescription;

import java.lang.reflect.Field;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SQLSetValue {

    private Field column;
    private Field rcolumn;
    private String value;

    //rcolumn field uses only in UPDATE statement, defined on column name in <val> parameter
    public SQLSetValue (String col, String val, Field[] cs, boolean isUpdate) throws InvalidColumnDescription {
        if (isUpdate) {
            rcolumn = null;
            for (int i=0; i<cs.length; i++) {
                if (val.toUpperCase().trim().equals(cs[i].getName().toUpperCase())) {
                    rcolumn = cs[i];
                    break;
                }
            }
        }
        for (int i=0; i<cs.length; i++) {
            if (col.toUpperCase().trim().equals(cs[i].getName().toUpperCase())) {
                column = cs[i];
                value  = val; //value filled by val always 
                return;
            }
        }
        throw new InvalidColumnDescription();
    }

    public Field getColumn() {
        return column;
    }

    public void setColumn(Field column) {
        this.column = column;
    }

    public Field getRcolumn() {
        return rcolumn;
    }

    public void setRcolumn(Field rcolumn) {
        this.rcolumn = rcolumn;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
    
}
