package com.alibaba.middleware.race.jstorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import clojure.lang.IFn;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.PaymentMessage;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

public class PaySpout implements IRichSpout {
    private static Logger LOG = LoggerFactory.getLogger(PaySpout.class);
    SpoutOutputCollector _collector;
    BlockingQueue<PaymentMessage> syncQueue;
    Random _rand;
    long sendingCount;
    long startTime;
    boolean isStatEnable;
    int taskId;

    DefaultMQPushConsumer payPushConsumer;
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
        sendingCount = 0;
        startTime = System.currentTimeMillis();
        isStatEnable = JStormUtils.parseBoolean(conf.get("is.stat.enable"), false);
        this.taskId = context.getThisTaskId();
        syncQueue = new SynchronousQueue();
        try{
            payPushConsumer = Consumer.createPushConsumer(RaceConfig.MetaConsumerGroup, RaceConfig.PAY);
            payPushConsumer.registerMessageListener(new MessageListenerConcurrently(){
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
                        PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
                        try{
                            syncQueue.put(paymentMessage);
                        }catch (InterruptedException e){
                            LOG.error("Interrupted Put Payment {}",paymentMessage,e);
                        }
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            payPushConsumer.start();
        }catch (MQClientException e){
            LOG.error("fail to initialize pay push consumer taskId:{}",taskId,e);
        }
        LOG.info("Prepare PaySpout taskId:{}",taskId);
    }

    @Override
    public void nextTuple() {
        try{
            PaymentMessage pay =  syncQueue.take();
            //副本发往去join
            _collector.emit(RaceConfig.orderFlowStream,new Values(pay.getOrderId(),RaceConfig.PAY,pay.getCreateTime(),pay.getPayAmount()));
            //副本发往去单独计算
            _collector.emit(RaceConfig.payStream,new Values(pay.getCreateTime(),pay.getPayPlatform(),pay.getPayAmount()));
        }catch (InterruptedException e){
            LOG.error("Interrupted Put Payment {}",e);
        }
        updateSendTps();
    }

    @Override
    public void ack(Object id) {
        // Ignored
    }
    @Override
    public void fail(Object id) {
        //_collector.emit(new Values(id), id);
        LOG.info("Pay fail id:{}",id);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(RaceConfig.payStream,new Fields("ctime","platform","amount"));
        declarer.declareStream(RaceConfig.orderFlowStream,new Fields("orderId","from","ctime","amount"));
    }

    private void updateSendTps() {
        if (!isStatEnable)
            return;
        sendingCount++;
        long now = System.currentTimeMillis();
        long interval = now - startTime;
        if (interval > 15 * 1000) {
            //LOG.info("Sending tps of last one minute is {} taskId:{}",(sendingCount * 1000) / interval,taskId);
            startTime = now;
            sendingCount = 0;
        }
    }

    @Override
    public void close() {
        LOG.info("shut down pay push consumer");
        payPushConsumer.shutdown();
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