package com.alibaba.middleware.race.util;

import java.io.RandomAccessFile;
import java.lang.reflect.Array;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by liuzhendong on 16/7/12.
 */
public class RecordCreator {

    static Random random = new Random();
    static AtomicLong counter = new AtomicLong();
    public final static String GOODS_FORMAT="goodid:%s salerid:%s good_name:%s price:%.2f offprice:%.2f";

    public static String createGoods(){
        return String.format(GOODS_FORMAT, UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                "good_" + System.currentTimeMillis()+"_" + counter.addAndGet(1),
                random.nextInt(1000) * Math.random(),
                random.nextInt(100) * Math.random());
    }

    public static void writeGoods(int num)throws Exception{
        RandomAccessFile randomAccessFile = new RandomAccessFile("/export/data/order/goods.txt","rw");
        randomAccessFile.seek(randomAccessFile.length());
        for(int i=0; i < num;i++){
            //randomAccessFile.write((createGoods()+"\n").getBytes("utf-8"));
            randomAccessFile.writeChars((createGoods()+"\n"));
        }
    }

    public static void read() throws Exception{
        RandomAccessFile randomAccessFile = new RandomAccessFile("/export/data/order/goods.txt","rw");
        for (int i = 0; i < 10; i++) {
            System.out.println(randomAccessFile.readByte());
        }
    }

    public static void main(String[] args)throws Exception{
        /*
        long start = System.currentTimeMillis();
        int num = 1000;
        writeGoods(num);
        long end = System.currentTimeMillis();
        System.out.println(String.format("num:%d cost:%d ms", num, end-start));
        */
        read();
    }

}
