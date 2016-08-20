package com.alibaba.middleware.race.model;

import java.util.Random;

public class TableItemFactory {

    private static final int BUYER_NUMS = 10000; //买家总数目
    private static final int PRODUCT_NUMS = 2000; //买家总数目

    private static final int MAX_TOTAL_PRICE = 10000 * 10;

    private static long startId = System.currentTimeMillis(); //第一个订单ID
    private static Random rand = new Random();



    private static int randInt(int max) {
        return rand.nextInt(max);
    }

    public static double round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();

        long factor = (long) Math.pow(10, places);
        value = value * factor;
        long tmp = Math.round(value);
        return (double) tmp / factor;
    }

    public static String createBuyerId() {
        return "buyer" + randInt(BUYER_NUMS);
    }

    public static long createOrderId() {
        return startId++;
    }

    public static String createProductId() {
        return "product" + randInt(PRODUCT_NUMS);
    }

    public static String createTbaoSalerId() {
        return "tb_saler" + randInt(BUYER_NUMS);
    }
    public static String createTmallSalerId() {
        return "tm_saler" + randInt(BUYER_NUMS);
    }

    public static double createTotalPrice() {
        return round(rand.nextDouble() * MAX_TOTAL_PRICE + 10, 2);
    }

}
