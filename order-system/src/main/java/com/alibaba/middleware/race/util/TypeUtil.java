package com.alibaba.middleware.race.util;

/**
 * Created by liuzhendong on 16/7/18.
 */
public class TypeUtil {
    public static byte[] longToBytes(long number) {
        long temp = number;
        byte[] b = new byte[8];
        for (int i = b.length - 1; i >=0; i--) {
            b[i] = (byte)(temp & 0xff);
            temp = temp >>> 8; // 向右移8位
        }
        return b;
    }

    public static byte[] intToBytes(int number) {
        int temp = number;
        byte[] b = new byte[4];
        for (int i = b.length - 1; i >=0; i--) {
            b[i] = (byte) (temp & 0xff);// 将最低位保存在最低位
            temp = temp >>> 8; // 向右移8位
        }
        return b;
    }

    public static byte[] shortToBytes(short number) {
        int temp = number;
        byte[] b = new byte[2];
        for (int i = b.length - 1; i >=0; i--) {
            b[i] = (byte) (temp & 0xff);
            temp = temp >>> 8; // 向右移8位
        }
        return b;
    }

    public static long bytesToLong(byte[] bytes){
        long res = 0;
        for (int i = 0; i < bytes.length; i++) {
            long  tmp = bytes[i] & 0xffL;
            res += tmp << ((bytes.length-i-1) * 8);
        }
        return res;
    }

}
