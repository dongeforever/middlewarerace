
package com.alibaba.middleware.race.rocketmq;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PayRatio;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;


/**
 * Producer，发送消息
 */
public class Producer {

    private static Random rand = new Random();
    private static int count = 1000;

    /**
     * 这是一个模拟堆积消息的程序，生成的消息模型和我们比赛的消息模型是一样的，
     * 所以选手可以利用这个程序生成数据，做线下的测试。
     * @param args
     * @throws MQClientException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws MQClientException, InterruptedException {
        if(args.length >= 1){
            count = Integer.valueOf(args[0]);
        }
        DefaultMQProducer producer = new DefaultMQProducer(RaceConfig.MetaProducerGroup);

        //在本地搭建好broker后,记得指定nameServer的地址
        producer.setNamesrvAddr(RaceConfig.RocketMQ_NAME_SERVER);

        producer.start();

        final String [] topics = new String[]{RaceConfig.MqTaobaoTradeTopic, RaceConfig.MqTmallTradeTopic};
        final Semaphore semaphore = new Semaphore(0);

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
                final OrderMessage orderMessage = ( platform == 0 ? OrderMessage.createTbaoMessage() : OrderMessage.createTmallMessage());
                orderMessage.setCreateTime(ctime);

                byte [] body = RaceUtils.writeKryoObject(orderMessage);

                Message msgToBroker = new Message(topics[platform], body);

                producer.send(msgToBroker, new SendCallback() {
                    public void onSuccess(SendResult sendResult) {
                        //System.out.println(orderMessage);
                        semaphore.release();
                    }
                    public void onException(Throwable throwable) {
                        throwable.printStackTrace();
                    }
                });
                //Send Pay message
                PaymentMessage[] paymentMessages = PaymentMessage.createPayMentMsg(orderMessage);
                double amount = 0;
                for (final PaymentMessage paymentMessage : paymentMessages) {
                    int retVal = Double.compare(paymentMessage.getPayAmount(), 0);
                    if (retVal < 0) {
                        throw new RuntimeException("price < 0 !!!!!!!!");
                    }
                    if(retVal == 0) continue;
                    if(platform == 0) {
                        Long key = RaceUtils.getMinuteTime(paymentMessage.getCreateTime());
                        if(tbSums.containsKey(key)){
                            tbSums.put(key, tbSums.get(key) + paymentMessage.getPayAmount());
                        }else {
                            tbSums.put(key,paymentMessage.getPayAmount());
                        }
                    }else {
                        Long key = RaceUtils.getMinuteTime(paymentMessage.getCreateTime());
                        if(tmSums.containsKey(key)){
                            tmSums.put(key, tmSums.get(key) + paymentMessage.getPayAmount());
                        }else {
                            tmSums.put(key, paymentMessage.getPayAmount());
                        }
                    }
                    Long key = RaceUtils.getMinuteTime(paymentMessage.getCreateTime());
                    PayRatio payRatio = new PayRatio().key(key).wxPay(paymentMessage.getPayPlatform() == 0 ? 0.0 : paymentMessage.getPayAmount())
                            .pcPay(paymentMessage.getPayPlatform() == 0 ? paymentMessage.getPayAmount() : 0.0);
                    if(payRatioMap.containsKey(key)){
                        payRatioMap.get(key).merge(payRatio);
                    }else {
                        payRatioMap.put(key,payRatio);
                    }

                    amount += paymentMessage.getPayAmount();
                    final Message messageToBroker = new Message(RaceConfig.MqPayTopic, RaceUtils.writeKryoObject(paymentMessage));
                    producer.send(messageToBroker, new SendCallback() {
                        public void onSuccess(SendResult sendResult) {
                            //System.out.println(paymentMessage);
                        }
                        public void onException(Throwable throwable) {
                            throwable.printStackTrace();
                        }
                    });
                }
                if (Double.compare(amount, orderMessage.getTotalPrice()) != 0) {
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

        semaphore.acquire(count);

        //用一个short标识生产者停止生产数据
        byte [] zero = new  byte[]{0,0};
        Message endMsgTB = new Message(RaceConfig.MqTaobaoTradeTopic, zero);
        Message endMsgTM = new Message(RaceConfig.MqTmallTradeTopic, zero);
        Message endMsgPay = new Message(RaceConfig.MqPayTopic, zero);

        try {
            producer.send(endMsgTB);
            producer.send(endMsgTM);
            producer.send(endMsgPay);
        } catch (Exception e) {
            e.printStackTrace();
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
        producer.shutdown();

    }
}
