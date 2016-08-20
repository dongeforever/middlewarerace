package com.alibaba.middleware.race.rocketmq;

import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.middleware.race.model.*;
import com.alibaba.middleware.race.RaceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Consumer，订阅消息
 */

/**
 * RocketMq消费组信息我们都会再正式提交代码前告知选手
 */
public class Consumer {
    private static Logger LOG = LoggerFactory.getLogger(Consumer.class);

    public static DefaultMQPushConsumer createDefaultPushConsumer(String consumerGroup)throws MQClientException{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        /**
         * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
         * 如果非第一次启动，那么按照上次消费的位置继续消费
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //在本地搭建好broker后,记得指定nameServer的地址
        if(RaceConfig.IS_TEST){
            consumer.setNamesrvAddr(RaceConfig.RocketMQ_NAME_SERVER);
        }
        consumer.subscribe(RaceConfig.MqPayTopic, "*");
        consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
        consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");

        consumer.setPullThresholdForQueue(1024);
        consumer.setPullInterval(0);
        consumer.setConsumeThreadMax(4);
        consumer.setConsumeThreadMin(4);
        consumer.setConsumeMessageBatchMaxSize(64);
        consumer.setPullBatchSize(64);


        LOG.info("consumeBatchSize:{} pullBatchSize:{} consumeThread:{}",consumer.getConsumeMessageBatchMaxSize(),consumer.getPullBatchSize(),consumer.getConsumeThreadMax());
        return consumer;
    }

    public static DefaultMQPushConsumer createPushConsumer(String consumerGroup,int type)throws MQClientException{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        /**
         * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
         * 如果非第一次启动，那么按照上次消费的位置继续消费
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //在本地搭建好broker后,记得指定nameServer的地址
        consumer.setNamesrvAddr(RaceConfig.RocketMQ_NAME_SERVER);

        switch (type){
            case  RaceConfig.PAY:
                consumer.subscribe(RaceConfig.MqPayTopic, "*");
                break;
            case RaceConfig.TBORDER:
                consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
                break;
            case RaceConfig.TMORDER:
                consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
                break;
            default:
                break;
        }
        return consumer;

    }


    public static void benchConsumerTest()throws MQClientException,InterruptedException{
        final BlockingQueue<PaymentMessage> paySyncQueue = new SynchronousQueue();
        final BlockingQueue<OrderMessage> tbSyncQueue =  new SynchronousQueue();
        final BlockingQueue<OrderMessage> tmSyncQueue = new SynchronousQueue();
        final BlockingQueue<Object> allQueue = new SynchronousQueue<>();
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try{
                    DefaultMQPushConsumer consumer = createDefaultPushConsumer(RaceConfig.MetaConsumerGroup);
                    consumer.registerMessageListener(new MessageListenerConcurrently(){
                        @Override
                        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                                        ConsumeConcurrentlyContext context) {
                            for (MessageExt msg : msgs) {
                                byte [] body = msg.getBody();
                                if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                                    //Info: 生产者停止生成数据, 并不意味着马上结束
                                    System.out.println("Got the end signal");
                                    continue;
                                }

                                //LOG.info("consumer topic:{} msgId:{} ts:{}",msg.getTopic(),msg.getMsgId(),Thread.currentThread().getId());
                                if(msg.getTopic().equals(RaceConfig.MqPayTopic)) {
                                    PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
                                    try{
                                        //paySyncQueue.put(paymentMessage);
                                        allQueue.put(paymentMessage);
                                    }catch (InterruptedException e){
                                        LOG.error("Interrupted Put Payment {}",paymentMessage,e);
                                    }
                                }else if(msg.getTopic().equals(RaceConfig.MqTaobaoTradeTopic)){
                                    OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
                                    try{
                                        //tbSyncQueue.put(orderMessage);
                                        allQueue.put(orderMessage);
                                    }catch (InterruptedException e){
                                        LOG.error("Interrupted Put TB Order {}", orderMessage,e);
                                    }
                                }else if(msg.getTopic().equals(RaceConfig.MqTmallTradeTopic)){
                                    OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
                                    try{
                                        //tmSyncQueue.put(orderMessage);
                                        allQueue.put(orderMessage);
                                    }catch (InterruptedException e){
                                        LOG.error("Interrupted Put TM Order {}", orderMessage,e);
                                    }
                                }else {
                                    LOG.info("unknown topic:{}",msg.getTopic());
                                }
                            }
                            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                        }
                    });
                    consumer.start();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        };

        new Thread(runnable).start();
        System.out.println("start");
        long start = System.currentTimeMillis();
        int sendNum=0, lastSendNum=0;
        while (true){
            Object object = allQueue.take();
            if(object != null)  sendNum++;
            /*
            PaymentMessage pay =  paySyncQueue.poll();
            if(pay != null){
                //System.out.println("PAY:" + pay.getOrderId());
                //Thread.sleep(2);
                sendNum++;
            }
            OrderMessage tmOrder = tmSyncQueue.poll();
            if(tmOrder != null){
                //System.out.println("TM:" + tmOrder.getOrderId());
                //Thread.sleep(2);
                sendNum++;
            }

            OrderMessage tbOrder = tbSyncQueue.poll();
            if(tbOrder != null){
                //System.out.println("TB:"+tbOrder.getOrderId());
                //Thread.sleep(2);
                sendNum++;
            }*/
            long now = System.currentTimeMillis();
            if(now - start > 1000){
                System.out.println(String.format("QPS:" + (sendNum-lastSendNum)));
                start = now;
                lastSendNum =sendNum;
            }
        }
    }


    public static void benchConsumerTest2()throws MQClientException,InterruptedException{
        final PayRatioArray payRatioArray = new PayRatioArray();
        payRatioArray.setMoveListener(new PayRatioArray.MoveListener() {
            @Override
            public void onMove(PayRatio payRatio) {
                if(payRatio.key == null) return;
                if(payRatio.pcPay > 0){
                    double ratio = RaceUtils.round(payRatio.wxPay/payRatio.pcPay);
                    System.out.println(String.format("%s=%f",RaceConfig.prex_ratio+payRatio.key, RaceUtils.round(ratio)));
                }
            }
        });

        final ItemArray tbPayMap = new ItemArray();
        final ItemArray tmPayMap = new ItemArray();
        tbPayMap.setMoveListener(new ItemArray.MoveListener(){
            @Override
            public void onMove(Item item){
                if(item.key == null) return;
                System.out.println(String.format("%s=%f",RaceConfig.prex_taobao+item.key,RaceUtils.round(item.amount)));
            }
        });
        tmPayMap.setMoveListener(new ItemArray.MoveListener() {
            @Override
            public void onMove(Item item) {
                if(item.key == null) return;
                System.out.println(String.format("%s=%f",RaceConfig.prex_tmall+item.key,RaceUtils.round(item.amount)));

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
        final  AtomicLong[] tps = {new AtomicLong(0),new AtomicLong(0),new AtomicLong(System.currentTimeMillis())};
        final AtomicLong orderNum = new AtomicLong(0);
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try{
                    DefaultMQPushConsumer consumer = createDefaultPushConsumer(RaceConfig.MetaConsumerGroup);
                    consumer.registerMessageListener(new MessageListenerConcurrently(){
                        @Override
                        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                                        ConsumeConcurrentlyContext context) {
                            for (MessageExt msg : msgs) {
                                byte [] body = msg.getBody();
                                if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                                    //Info: 生产者停止生成数据, 并不意味着马上结束
                                    System.out.println("Got the end signal");
                                    continue;
                                }
                                /*
                                //LOG.info("consumer topic:{} msgId:{} ts:{}",msg.getTopic(),msg.getMsgId(),Thread.currentThread().getId());
                                if(msg.getTopic().equals(RaceConfig.MqPayTopic)) {
                                    PaymentMessage pay = RaceUtils.readKryoObject(PaymentMessage.class, body);
                                    if(pay.getPayPlatform() == 0){
                                        payRatioArray.merge(new PayRatio().key(RaceUtils.getMinuteTime(pay.getCreateTime())).pcPay(pay.getPayAmount()));
                                    }else if(pay.getPayPlatform() == 1){
                                        payRatioArray.merge(new PayRatio().key(RaceUtils.getMinuteTime(pay.getCreateTime())).wxPay(pay.getPayAmount()));
                                    }
                                    tps[0].addAndGet(1);
                                    orderJoinMap.merge(new OrderJoinItem(pay.getOrderId()).from(RaceConfig.PAY).totalPay(pay.getPayAmount()).ctime(pay.getCreateTime()));
                                }else if(msg.getTopic().equals(RaceConfig.MqTaobaoTradeTopic)){
                                    OrderMessage order = RaceUtils.readKryoObject(OrderMessage.class, body);
                                    orderJoinMap.merge(new OrderJoinItem(order.getOrderId()).from(RaceConfig.TBORDER).totalPrice(order.getTotalPrice()).ctime(order.getCreateTime()));
                                    tps[0].addAndGet(1);
                                    orderNum.addAndGet(1);
                                }else if(msg.getTopic().equals(RaceConfig.MqTmallTradeTopic)){
                                    OrderMessage order = RaceUtils.readKryoObject(OrderMessage.class, body);
                                    orderJoinMap.merge(new OrderJoinItem(order.getOrderId()).from(RaceConfig.TMORDER).totalPrice(order.getTotalPrice()).ctime(order.getCreateTime()));
                                    tps[0].addAndGet(1);
                                    orderNum.addAndGet(1);
                                }else {
                                    LOG.info("unknown topic:{}",msg.getTopic());
                                }
                                */
                                tps[0].addAndGet(1);
                                long now = System.currentTimeMillis();
                                if(now - tps[2].get() > 2 * 1000){
                                    synchronized (Consumer.class){
                                        if(now - tps[2].get() > 2 * 1000){
                                            System.out.println(String.format("CONSUMER_TPS tps:%d ts:%d",(tps[0].get()-tps[1].get())* 1000/(now - tps[2].get()), Thread.currentThread().getId()));
                                            tps[1].set(tps[0].get());
                                            tps[2].set(now);
                                        }
                                    }
                                }

                            }
                            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                        }
                    });
                    consumer.start();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        };
        new Thread(runnable).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                int lastSize = 0;
                int equalTime=0;
                while (true){
                    try {
                        Thread.sleep(2000);
                        System.out.println(String.format("OrderJoinMapSize:%d orderSize:%d",orderJoinMap.joinMap.size(),orderNum.get()));
                        if(orderJoinMap.joinMap.size() == lastSize){
                            equalTime++;
                            if(equalTime > 3){
                                for(Long key:orderJoinMap.joinMap.keySet()){
                                    System.out.println(String.format("Key:%d Value:%s",key,orderJoinMap.joinMap.get(key)));
                                }
                            }
                        }else {
                            equalTime = 0;
                        }
                        lastSize = orderJoinMap.joinMap.size();
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }

            }
        }).start();
        System.out.println("start");
        new CountDownLatch(1).await();
    }
    public static void main(String[] args) throws InterruptedException, MQClientException {
        if(args.length > 0){
            benchConsumerTest();
        }else {
            benchConsumerTest2();

        }
    }
}
