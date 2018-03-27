package com.taobao.yugong.common.utils;/**
 * created by Intellij IDEA
 * User: yiqiang-zhang
 * Date: 2018-03-27
 * Time: 16:44
 */

import java.util.HashSet;
import java.util.Set;

/**
 * mysql 中关键字匹配。
 * Author: zhangyq<p>
 * Date: 16:44 2018-03-27 <p>
 * Description: <p>
 */
public class Keywords {

    private static Set<String> keywords = new HashSet();
    static {
        keywords.add("sql");
        keywords.add("condition");
        keywords.add("key");
    }

    public static boolean contains(String name) {
        return keywords.contains(name);
    }
}
