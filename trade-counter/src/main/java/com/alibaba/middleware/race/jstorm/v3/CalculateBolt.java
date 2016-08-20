package com.alibaba.middleware.race.jstorm.v3;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.*;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class CalculateBolt implements IRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(CalculateBolt.class);
    OutputCollector _collector;
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

    Map<String,Values> flushMap;

    final  AtomicLong[] tps = {new AtomicLong(0),new AtomicLong(0),new AtomicLong(System.currentTimeMillis())};

    final BlockingQueue<Object> valuesQueue = new ArrayBlockingQueue<>(5000);
    final int tsNum = 2;


    final BlockingQueue<Values> flushQueue = new ArrayBlockingQueue<Values>(1000);
    @Override
    public void prepare(Map conf, TopologyContext context, final OutputCollector collector){
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
                        flushQueue.put(new Values(RaceConfig.RATIO, payRatio.key, ratio));
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
                    flushQueue.put(new Values(RaceConfig.TB_PAY, item.key, item.amount));
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
                    flushQueue.put(new Values(RaceConfig.TM_PAY, item.key, item.amount));
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
        Runnable deserialTask = new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        List values = (List) valuesQueue.take();
                        for (Object obj : values) {
                            MetaMsg msg = (MetaMsg) obj;
                            if (msg.topic == RaceConfig.PAY) {
                                PaymentMessage pay = RaceUtils.readKryoObject(PaymentMessage.class, msg.body);
                                if (pay.getPayPlatform() == 0) {
                                    payRatioArray.merge(new PayRatio().key(RaceUtils.getMinuteTime(pay.getCreateTime())).pcPay(pay.getPayAmount()));
                                } else if (pay.getPayPlatform() == 1) {
                                    payRatioArray.merge(new PayRatio().key(RaceUtils.getMinuteTime(pay.getCreateTime())).wxPay(pay.getPayAmount()));
                                }
                                orderJoinMap.merge(new OrderJoinItem(pay.getOrderId()).from(RaceConfig.PAY).totalPay(pay.getPayAmount()).ctime(pay.getCreateTime()));
                            } else if (msg.topic == RaceConfig.TBORDER) {
                                OrderMessage order = RaceUtils.readKryoObject(OrderMessage.class, msg.body);
                                orderJoinMap.merge(new OrderJoinItem(order.getOrderId()).from(RaceConfig.TBORDER).totalPrice(order.getTotalPrice()).ctime(order.getCreateTime()));
                            } else if (msg.topic == RaceConfig.TMORDER) {
                                OrderMessage order = RaceUtils.readKryoObject(OrderMessage.class, msg.body);
                                orderJoinMap.merge(new OrderJoinItem(order.getOrderId()).from(RaceConfig.TMORDER).totalPrice(order.getTotalPrice()).ctime(order.getCreateTime()));
                            }
                            tps[0].addAndGet(1);
                            if (isStatEnable) {
                                long now = System.currentTimeMillis();
                                if (now - tps[2].get() > 2 * 1000) {
                                    synchronized (CalculateBolt.class) {
                                        if (now - tps[2].get() > 2 * 1000) {
                                            LOG.info(String.format("DESE_TPS tps:%d ts:%d", (tps[0].get() - tps[1].get()) * 1000 / (now - tps[2].get()), Thread.currentThread().getId()));
                                            tps[1].set(tps[0].get());
                                            tps[2].set(now);
                                        }
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        };

        Thread[] ts = new Thread[tsNum];
        for(int i = 0; i < ts.length; i++){
            ts[i] = new Thread(deserialTask);
        }
        for(int i = 0; i < ts.length; i++){
            ts[i].start();
        }
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true){
                    try{
                        collector.emit(flushQueue.take());
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
            }
        }).start();
        LOG.info("Prepare %s taskId:{} tsNum:%d", this.getClass().getName(), taskId, tsNum);
    }

    @Override
    public void execute(Tuple tuple){
        try {
            valuesQueue.put(tuple.getValue(0));
            _collector.ack(tuple);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("type","key","value"));
    }
    @Override
    public void cleanup() {
        // TODO Auto-generated method stub

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}