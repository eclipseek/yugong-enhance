package com.taobao.yugong.common.utils;/**
 * created by Intellij IDEA
 * User: yiqiang-zhang
 * Date: 2018-03-24
 * Time: 11:13
 */

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * Author: zhangyq<p>
 * Date: 11:13 2018-03-24 <p>
 * Description: <p>
 */
public class TypeMapping {

    private static final Map<Integer, String> types = new HashMap<Integer, String>();
    static {
        types.put(Types.INTEGER, "INTEGER");
        types.put(Types.TINYINT, "TINYINT");
        types.put(Types.SMALLINT, "SMALLINT");
        types.put(Types.BIGINT, "BIGINT");
        types.put(Types.DECIMAL, "DECIMAL");
        types.put(Types.BIT, "BIT");
        types.put(Types.REAL, "REAL");
        types.put(Types.DATE, "DATE");
        types.put(Types.TIME, "TIME");
        types.put(Types.TIMESTAMP, "DATETIME");
        types.put(Types.BLOB, "BLOB");
        types.put(Types.CLOB, "TEXT");
        types.put(Types.CHAR, "CHAR");
        types.put(Types.VARCHAR, "VARCHAR");
        types.put(Types.NUMERIC, "NUMERIC");
        types.put(Types.FLOAT, "FLOAT");
    }

    public static String get(int type) {
        return types.get(type);
    }

}
