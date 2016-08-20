package com.alibaba.middleware.race.jstorm.v3;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
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
import java.util.concurrent.atomic.AtomicLong;

public class BatchSpout implements IRichSpout {
    private static Logger LOG = LoggerFactory.getLogger(BatchSpout.class);
    SpoutOutputCollector _collector;
    Random _rand;
    long sendingCount;
    long lastSendingCount;
    long startTime;
    boolean isStatEnable;
    int taskId;


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
        try{
            allConsumer = Consumer.createDefaultPushConsumer(RaceConfig.MetaConsumerGroup);
            allConsumer.registerMessageListener(new MessageListenerConcurrently(){
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                                ConsumeConcurrentlyContext context) {
                    Values values = new Values();
                    for (MessageExt msg : msgs) {
                        byte [] body = msg.getBody();
                        if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                            System.out.println("Got the end signal");
                            continue;
                        }
                        //LOG.info("consumer topic:{} msgId:{}",msg.getTopic(),msg.getMsgId());
                        if(msg.getTopic().equals(RaceConfig.MqPayTopic)) {
                            values.add(new MetaMsg().topic(RaceConfig.PAY).body(msg.getBody()));
                        }else if(msg.getTopic().equals(RaceConfig.MqTaobaoTradeTopic)){
                            values.add(new MetaMsg().topic(RaceConfig.TBORDER).body(msg.getBody()));
                        }else if(msg.getTopic().equals(RaceConfig.MqTmallTradeTopic)){
                            values.add(new MetaMsg().topic(RaceConfig.TMORDER).body(msg.getBody()));
                        }else {
                            LOG.info("unknown topic:{}",msg.getTopic());
                        }
                        tps[0].addAndGet(1);
                        if(isStatEnable){
                            long now = System.currentTimeMillis();
                            if(now - tps[2].get() > 2 * 1000){
                                synchronized (BatchSpout.class){
                                    if(now - tps[2].get() > 2 * 1000){
                                        LOG.info(String.format("CONSUMER_TPS tps:%d ts:%d",(tps[0].get()-tps[1].get())* 1000/(now - tps[2].get()), Thread.currentThread().getId()));
                                        tps[1].set(tps[0].get());
                                        tps[2].set(now);
                                    }
                                }
                            }
                        }
                    }
                    if(values.size() > 0) valuesQueue.add(values);
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
            _collector.emit(new Values(values));
        }catch (Exception e){
            e.printStackTrace();
        }
        //updateSendTps();
    }

    @Override
    public void ack(Object id) {

    }
    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("values"));
    }

    private void updateSendTps() {
        if (!isStatEnable)
            return;
        long now = System.currentTimeMillis();
        long interval = now - startTime;
        long count = sendingCount - lastSendingCount;
        if (interval > 3 * 1000) {
            LOG.info("BatchSendTps is {} taskId:{}",(count * 1000) / interval,taskId);
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