package com.qcc.utils;


/*
 *
 * @author liukai
 * @date 2026/5/6 16:59
 */

public class EntityUtil {
    // 18位统一信用代码=企业，否则=自然人
    public static boolean isNaturalPerson(String code) {
        return code == null || code.length() != 18;
    }
}
