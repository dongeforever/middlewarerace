package com.alibaba.middleware.race.store;

import java.util.concurrent.Semaphore;

/**
 * Created by liuzhendong on 16/7/15.
 */
public class StoreConfig {

    //上线记得修改这个地方的配置
    public static final int DEFAULT_PAGE_SIZE = 20 * 1024;  //默认的page大小
    public static final int MAX_LEAF_IN_MEMORY_PER_BTREE = 1024;  //每棵B树允许在内存中的叶子节点数
    public static final int MAX_KV_IN_MEMORY_PER_BTREE = 1024 * 1024;  //每棵B树允许在内存中的叶子节点数
    public static final int DEFAULT_AREA_SIZE = 64 * 1024 * 1024; //默认的分区大小
    public static final int DEFALT_BTREE_DISK_COST = 1024 * 1024;//默认b树所占磁盘的大小



    //simple btree
    public static final int DEFAULT_BUFF_LEN = 1024 * 1024;
    public static final int DEFAULT_ORIGIN_BUFF_LEN = 1024 * 1024;

    //分表个数
    public static final int ORDER_PARTITION_NUM = 40; //40
    public static final int ORDER_BUILD_THREAD_NUM = 10; //10

    public static final int ORDER_ORIGIN_STORE_NUM = 1000; //线上弄到1000

    public static final Semaphore STREE_CHECK_SEMAPHORE = new Semaphore(4); //4
}
