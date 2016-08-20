package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {
    //flush类型
    public static final int RATIO = 1;
    public static final int TM_PAY = 2;
    public static final int TB_PAY = 3;
    //类型
    public static final int PAY = 0;
    public static final int TBORDER = 1;
    public static final int TMORDER = 2;
    //stream
    public static String payStream = "pay_stream";
    public static String orderFlowStream = "order_flow_stream";



    //这些是写tair key的前缀
    public static String prex_tmall = "platformTmall_45416mlava_";
    public static String prex_taobao = "platformTaobao_45416mlava_";
    public static String prex_ratio = "ratio_45416mlava_";


    //这些jstorm/rocketMq/tair 的集群配置信息，这些配置信息在正式提交代码前会被公布
    public static String JstormTopologyName = "45416mlava";
    public static String MetaProducerGroup = "DovaProducer";
    public static String MetaConsumerGroup = "45416mlava";


    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";





    public static String TairConfigServer = "10.101.72.127:5198";
    public static String TairSalveConfigServer = "10.101.72.128:5198";
    public static String TairGroup = "group_tianchi";
    public static Integer TairNamespace = 33162;
    public static boolean IS_TEST = false;





    /*
    public static boolean IS_TEST = true;
    public static String TairConfigServer = "101.200.121.18:5198";
    public static String TairSalveConfigServer = "101.200.121.18:5198";
    public static String TairGroup = "group_1";
    public static Integer TairNamespace = 1;
    */

    public static final String RocketMQ_NAME_SERVER = "182.92.175.210:9876";
}
