package com.alibaba.middleware.race.jstorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.*;
import com.alibaba.middleware.race.rocketmq.Consumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class AllInOneSpoutV2 implements IRichSpout {
    private static Logger LOG = LoggerFactory.getLogger(AllInOneSpoutV2.class);
    SpoutOutputCollector _collector;
    Random _rand;
    long sendingCount;
    long lastSendingCount;
    long startTime;
    boolean isStatEnable;
    int taskId;

    //计算相关
    PayRatioArray payRatioArray;
    ItemArray tbPayArray;
    ItemArray tmPayArray;
    OrderJoinMap orderJoinMap;
    OrderJoinMap orderJoinMap2;


    Map<String,Values> flushMap;

    final  AtomicLong[] tps = {new AtomicLong(0),new AtomicLong(0),new AtomicLong(System.currentTimeMillis())};
    DefaultMQPushConsumer allConsumer;

    final BlockingQueue<Values> valuesQueue = new ArrayBlockingQueue<Values>(1000);
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
        sendingCount = 0;
        lastSendingCount = 0;
        startTime = System.currentTimeMillis();
        isStatEnable = JStormUtils.parseBoolean(conf.get("is.stat.enable"), false);
        this.taskId = context.getThisTaskId();
        flushMap = new ConcurrentHashMap<>(1024);
        payRatioArray = new PayRatioArray();
        payRatioArray.setMoveListener(new PayRatioArray.MoveListener() {
            @Override
            public void onMove(PayRatio payRatio) {
                if(payRatio.key == null) return;
                if(payRatio.pcPay > 0){
                    double ratio = RaceUtils.round(payRatio.wxPay/payRatio.pcPay);
                    //System.out.println(String.format("key:%s value:%.2f",RaceConfig.prex_ratio+payRatio.key,ratio));
                    try{
                        valuesQueue.put(new Values(RaceConfig.RATIO, payRatio.key, ratio));
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
            }
        });

        tbPayArray = new ItemArray();
        tmPayArray = new ItemArray();
        tbPayArray.setMoveListener(new ItemArray.MoveListener() {
            @Override
            public void onMove(Item item) {
                //System.out.println(String.format("key:%s value:%.2f",RaceConfig.prex_taobao+key,item.amount));
                if(item.key == null) return;
                try {
                    valuesQueue.put(new Values(RaceConfig.TB_PAY, item.key, item.amount));
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        });
        tmPayArray.setMoveListener(new ItemArray.MoveListener() {
            @Override
            public void onMove(Item item) {
                //System.out.println(String.format("key:%s value:%.2f",RaceConfig.prex_tmall+key,item.amount));
                if(item.key == null) return;
                try {
                    valuesQueue.put(new Values(RaceConfig.TM_PAY, item.key, item.amount));
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        });
        orderJoinMap = new OrderJoinMap();
        orderJoinMap.joinMeetListener(new OrderJoinMap.JoinMeetListener() {
            @Override
            public void onMeet(OrderJoinItem item) {
                if(item.payItems == null){
                    LOG.error("ON_MEET_ERROR key:{}",item.orderId);
                }
                for (OrderJoinItem payItem: item.payItems){
                    if(item.from == RaceConfig.TBORDER ){
                        tbPayArray.merge(new Item().amount(payItem.totalPay).key(RaceUtils.getMinuteTime(payItem.ctime)));
                    }else if (item.from == RaceConfig.TMORDER){
                        tmPayArray.merge(new Item().amount(payItem.totalPay).key(RaceUtils.getMinuteTime(payItem.ctime)));
                    }
                }
            }
        });
        orderJoinMap2 = new OrderJoinMap();
        orderJoinMap2.joinMeetListener(new OrderJoinMap.JoinMeetListener() {
            @Override
            public void onMeet(OrderJoinItem item) {
                if(item.payItems == null){
                    LOG.error("ON_MEET_ERROR key:{}",item.orderId);
                }
                for (OrderJoinItem payItem: item.payItems){
                    if(item.from == RaceConfig.TBORDER ){
                        tbPayArray.merge(new Item().amount(payItem.totalPay).key(RaceUtils.getMinuteTime(payItem.ctime)));
                    }else if (item.from == RaceConfig.TMORDER){
                        tmPayArray.merge(new Item().amount(payItem.totalPay).key(RaceUtils.getMinuteTime(payItem.ctime)));
                    }
                }
            }
        });
        try{
            allConsumer = Consumer.createDefaultPushConsumer(RaceConfig.MetaConsumerGroup);
            allConsumer.registerMessageListener(new MessageListenerConcurrently(){
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
                        //LOG.info("consumer topic:{} msgId:{}",msg.getTopic(),msg.getMsgId());
                        if(msg.getTopic().equals(RaceConfig.MqPayTopic)) {
                            PaymentMessage pay = RaceUtils.readKryoObject(PaymentMessage.class, body);
                            if(pay.getPayPlatform() == 0){
                                payRatioArray.merge(new PayRatio().key(RaceUtils.getMinuteTime(pay.getCreateTime())).pcPay(pay.getPayAmount()));
                            }else if(pay.getPayPlatform() == 1){
                                payRatioArray.merge(new PayRatio().key(RaceUtils.getMinuteTime(pay.getCreateTime())).wxPay(pay.getPayAmount()));
                            }
                            if(pay.getOrderId() % 2 == 0){
                                orderJoinMap.merge(new OrderJoinItem(pay.getOrderId()).from(RaceConfig.PAY).totalPay(pay.getPayAmount()).ctime(pay.getCreateTime()));
                            }else {
                                orderJoinMap2.merge(new OrderJoinItem(pay.getOrderId()).from(RaceConfig.PAY).totalPay(pay.getPayAmount()).ctime(pay.getCreateTime()));
                            }
                            tps[0].addAndGet(1);
                        }else if(msg.getTopic().equals(RaceConfig.MqTaobaoTradeTopic)){
                            OrderMessage order = RaceUtils.readKryoObject(OrderMessage.class, body);
                            if(order.getOrderId() % 2 == 0){
                                orderJoinMap.merge(new OrderJoinItem(order.getOrderId()).from(RaceConfig.TBORDER).totalPrice(order.getTotalPrice()).ctime(order.getCreateTime()));
                            }else{
                                orderJoinMap2.merge(new OrderJoinItem(order.getOrderId()).from(RaceConfig.TBORDER).totalPrice(order.getTotalPrice()).ctime(order.getCreateTime()));
                            }
                            tps[0].addAndGet(1);
                        }else if(msg.getTopic().equals(RaceConfig.MqTmallTradeTopic)){
                            OrderMessage order = RaceUtils.readKryoObject(OrderMessage.class, body);
                            if(order.getOrderId() % 2 == 0){
                                orderJoinMap.merge(new OrderJoinItem(order.getOrderId()).from(RaceConfig.TMORDER).totalPrice(order.getTotalPrice()).ctime(order.getCreateTime()));
                            }else {
                                orderJoinMap2.merge(new OrderJoinItem(order.getOrderId()).from(RaceConfig.TMORDER).totalPrice(order.getTotalPrice()).ctime(order.getCreateTime()));
                            }
                            tps[0].addAndGet(1);
                        }else {
                            LOG.info("unknown topic:{}",msg.getTopic());
                        }
                        if(isStatEnable){
                            long now = System.currentTimeMillis();
                            if(now - tps[2].get() > 2 * 1000){
                                synchronized (AllInOneSpoutV2.class){
                                    if(now - tps[2].get() > 2 * 1000){
                                        LOG.info(String.format("CONSUMER_TPS tps:%d ts:%d",(tps[0].get()-tps[1].get())* 1000/(now - tps[2].get()), Thread.currentThread().getId()));
                                        tps[1].set(tps[0].get());
                                        tps[2].set(now);
                                    }
                                }
                            }
                        }
                    }

                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            allConsumer.start();
        }catch (MQClientException e){
            LOG.error("fail to initialize all push consumer taskId:{}",taskId,e);
        }
        LOG.info("Prepare AllInOneSpout taskId:{}",taskId);
    }

    @Override
    public void nextTuple() {
        try {
            Values values = valuesQueue.take();
            String key = values.get(0) +"_" + values.get(1);
            flushMap.put(key,values);
            _collector.emit(values,key);
        }catch (Exception e){
            e.printStackTrace();
        }
        updateSendTps();
    }

    @Override
    public void ack(Object id) {

    }
    @Override
    public void fail(Object id) {
        //失败,则重新发送
        try {
            _collector.emit(flushMap.get(id),id);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("type","key","value"));

    }

    private void updateSendTps() {
        if (!isStatEnable)
            return;
        long now = System.currentTimeMillis();
        long interval = now - startTime;
        long count = sendingCount - lastSendingCount;
        if (interval > 3 * 1000) {
            LOG.info("ALL_IN_ONE_SPOUT_SENDING_TPS is {} taskId:{}",(count * 1000) / interval,taskId);
            startTime = now;
            lastSendingCount = sendingCount;
        }
    }

    @Override
    public void close() {
        LOG.info("shut down all push consumer");
        allConsumer.shutdown();
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {


    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}