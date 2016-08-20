package com.alibaba.middleware.race.util;

import java.lang.ref.SoftReference;

/**
 * Created by liuzhendong on 16/7/19.
 */
public class PrintUtil {

    public static void print(String format, Object... args){
        System.out.println(String.format(format, args));
    }
}
