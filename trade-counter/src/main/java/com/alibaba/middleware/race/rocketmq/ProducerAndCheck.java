
package com.alibaba.middleware.race.rocketmq;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.*;
import com.alibaba.rocketmq.client.exception.MQClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;


/**
 * Producer，发送消息
 */
public class ProducerAndCheck {

    private static Random rand = new Random();
    private static int count = 1000;
    private static Logger LOG = LoggerFactory.getLogger(ProducerAndCheck.class);


    public static void addAndCheck() throws MQClientException,InterruptedException{
        final PayRatioArray payRatioArray = new PayRatioArray();
        payRatioArray.setMoveListener(new PayRatioArray.MoveListener() {
            @Override
            public void onMove(PayRatio payRatio) {
                if(payRatio.key == null) return;
                if(payRatio.pcPay > 0){
                    double ratio = RaceUtils.round(payRatio.wxPay/payRatio.pcPay);
                    System.out.println(String.format("CHECK %s=%f",RaceConfig.prex_ratio+payRatio.key, RaceUtils.round(ratio)));
                }
            }
        });

        final ItemArray tbPayMap = new ItemArray();
        final ItemArray tmPayMap = new ItemArray();
        tbPayMap.setMoveListener(new ItemArray.MoveListener(){
            @Override
            public void onMove(Item item){
                if(item.key == null) return;
                System.out.println(String.format("CHECK %s=%f",RaceConfig.prex_taobao+item.key,RaceUtils.round(item.amount)));
            }
        });
        tmPayMap.setMoveListener(new ItemArray.MoveListener() {
            @Override
            public void onMove(Item item) {
                if(item.key == null) return;
                System.out.println(String.format("CHECK %s=%f",RaceConfig.prex_tmall+item.key,RaceUtils.round(item.amount)));

            }
        });
        final OrderJoinMap orderJoinMap = new OrderJoinMap();
        orderJoinMap.joinMeetListener(new OrderJoinMap.JoinMeetListener() {
            @Override
            public void onMeet(OrderJoinItem item) {
                if(item.payItems == null){
                    LOG.error("ON_MEET_ERROR key:{}",item.orderId);
                }
                for (OrderJoinItem payItem: item.payItems){
                    if(item.from == RaceConfig.TBORDER ){
                        tbPayMap.merge(new Item().amount(payItem.totalPay).key(RaceUtils.getMinuteTime(payItem.ctime)));
                    }else if (item.from == RaceConfig.TMORDER){
                        tmPayMap.merge(new Item().amount(payItem.totalPay).key(RaceUtils.getMinuteTime(payItem.ctime)));
                    }
                }
            }
        });

        final String [] topics = new String[]{RaceConfig.MqTaobaoTradeTopic, RaceConfig.MqTmallTradeTopic};

        long base = System.currentTimeMillis();
        Map<Long,Double> tbSums = new HashMap<>();
        Map<Long,Double> tmSums = new HashMap<>();
        long ctime = base;
        long split = 180;
        Map<Long,PayRatio> payRatioMap = new TreeMap<>();
        long[] tps = {0,0,System.currentTimeMillis()};
        for (int i = 0; i < count; i++) {
            try {
                if(i % (count/split) == 0){
                    base = base + 60 * 1000;
                }
                ctime =  base + rand.nextInt(2 * 60 * 1000);
                //ctime += 1000 + rand.nextInt();

                final int platform = rand.nextInt(2);
                final OrderMessage order = ( platform == 0 ? OrderMessage.createTbaoMessage() : OrderMessage.createTmallMessage());
                order.setCreateTime(ctime);

                if(platform == 0){
                    orderJoinMap.merge(new OrderJoinItem(order.getOrderId()).from(RaceConfig.TBORDER).totalPrice(order.getTotalPrice()).ctime(order.getCreateTime()));
                }else {
                    orderJoinMap.merge(new OrderJoinItem(order.getOrderId()).from(RaceConfig.TMORDER).totalPrice(order.getTotalPrice()).ctime(order.getCreateTime()));
                }
                //Send Pay message
                PaymentMessage[] paymentMessages = PaymentMessage.createPayMentMsg(order);
                double amount = 0;
                for (final PaymentMessage pay : paymentMessages) {
                    int retVal = Double.compare(pay.getPayAmount(), 0);
                    if (retVal < 0) {
                        throw new RuntimeException("price < 0 !!!!!!!!");
                    }
                    if(retVal == 0) continue;
                    if(pay.getPayPlatform() == 0){
                        payRatioArray.merge(new PayRatio().key(RaceUtils.getMinuteTime(pay.getCreateTime())).pcPay(pay.getPayAmount()));
                    }else if(pay.getPayPlatform() == 1){
                        payRatioArray.merge(new PayRatio().key(RaceUtils.getMinuteTime(pay.getCreateTime())).wxPay(pay.getPayAmount()));
                    }
                    orderJoinMap.merge(new OrderJoinItem(pay.getOrderId()).from(RaceConfig.PAY).totalPay(pay.getPayAmount()).ctime(pay.getCreateTime()));


                    if(platform == 0) {
                        Long key = RaceUtils.getMinuteTime(pay.getCreateTime());
                        if(tbSums.containsKey(key)){
                            tbSums.put(key, tbSums.get(key) + pay.getPayAmount());
                        }else {
                            tbSums.put(key, pay.getPayAmount());
                        }
                    }else {
                        Long key = RaceUtils.getMinuteTime(pay.getCreateTime());
                        if(tmSums.containsKey(key)){
                            tmSums.put(key, tmSums.get(key) + pay.getPayAmount());
                        }else {
                            tmSums.put(key, pay.getPayAmount());
                        }
                    }
                    Long key = RaceUtils.getMinuteTime(pay.getCreateTime());
                    PayRatio payRatio = new PayRatio().key(key).wxPay(pay.getPayPlatform() == 0 ? 0.0 : pay.getPayAmount())
                            .pcPay(pay.getPayPlatform() == 0 ? pay.getPayAmount() : 0.0);
                    if(payRatioMap.containsKey(key)){
                        payRatioMap.get(key).merge(payRatio);
                    }else {
                        payRatioMap.put(key,payRatio);
                    }
                    amount += pay.getPayAmount();
                }
                if (Double.compare(amount, order.getTotalPrice()) != 0) {
                    throw new RuntimeException("totalprice is not equal.");
                }
                tps[0]++;
                long now = System.currentTimeMillis();
                if(now - tps[2] > 2 * 1000){
                    System.out.println(String.format("TPS:%d",(tps[0]-tps[1])*1000/(now-tps[2])));
                    tps[2] = now;
                    tps[1] = tps[0];
                }
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }
        //产生数据,用来做校验
        for (Long key : tbSums.keySet()){
            System.out.println(String.format("%s=%f", RaceConfig.prex_taobao+key, RaceUtils.round(tbSums.get(key))));
        }
        for (Long key : tmSums.keySet()){
            System.out.println(String.format("%s=%f",RaceConfig.prex_tmall+key, RaceUtils.round(tmSums.get(key))));
        }
        double wxTotal = 0.0;
        double pcTotal = 0.0;
        for (Long key : payRatioMap.keySet()){
            PayRatio payRatio = payRatioMap.get(key);
            wxTotal += payRatio.wxPay;
            pcTotal += payRatio.pcPay;
            if(pcTotal > 0)
                System.out.println(String.format("%s=%f",RaceConfig.prex_ratio+key, RaceUtils.round(wxTotal/pcTotal)));
        }
    }
    public static void main(String[] args) throws MQClientException, InterruptedException {
        if(args.length > 0){
            count = Integer.valueOf(args[0]);
        }

        addAndCheck();
    }
}
