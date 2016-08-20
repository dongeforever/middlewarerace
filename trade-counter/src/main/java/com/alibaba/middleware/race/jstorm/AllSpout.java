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
import com.alibaba.middleware.race.model.OrderMessage;
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

public class AllSpout implements IRichSpout {
    private static Logger LOG = LoggerFactory.getLogger(AllSpout.class);
    SpoutOutputCollector _collector;
    BlockingQueue<PaymentMessage> paySyncQueue;
    BlockingQueue<OrderMessage> tbSyncQueue;
    BlockingQueue<OrderMessage> tmSyncQueue;


    Random _rand;
    long sendingCount;
    long lastSendingCount;
    long startTime;
    boolean isStatEnable;
    int taskId;

    DefaultMQPushConsumer allConsumer;
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
        sendingCount = 0;
        lastSendingCount = 0;
        startTime = System.currentTimeMillis();
        isStatEnable = JStormUtils.parseBoolean(conf.get("is.stat.enable"), false);
        this.taskId = context.getThisTaskId();
        paySyncQueue = new SynchronousQueue();
        tbSyncQueue =  new SynchronousQueue();
        tmSyncQueue = new SynchronousQueue();
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
                            PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
                            try{
                                paySyncQueue.put(paymentMessage);
                            }catch (InterruptedException e){
                                LOG.error("Interrupted Put Payment {}",paymentMessage,e);
                            }
                        }else if(msg.getTopic().equals(RaceConfig.MqTaobaoTradeTopic)){
                            OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
                            try{
                                tbSyncQueue.put(orderMessage);
                            }catch (InterruptedException e){
                                LOG.error("Interrupted Put TB Order {}", orderMessage,e);
                            }
                        }else if(msg.getTopic().equals(RaceConfig.MqTmallTradeTopic)){
                            OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
                            try{
                                tmSyncQueue.put(orderMessage);
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
            allConsumer.start();
        }catch (MQClientException e){
            LOG.error("fail to initialize pay push consumer taskId:{}",taskId,e);
        }
        LOG.info("Prepare PaySpout taskId:{}",taskId);
    }

    @Override
    public void nextTuple() {
        PaymentMessage pay =  paySyncQueue.poll();
        if(pay != null){
            //副本发往去join
            _collector.emit(RaceConfig.orderFlowStream,new Values(pay.getOrderId(),RaceConfig.PAY,pay.getCreateTime(),pay.getPayAmount()));
            //副本发往去单独计算
            _collector.emit(RaceConfig.payStream,new Values(pay.getCreateTime(),pay.getPayPlatform(),pay.getPayAmount()));
            sendingCount++;
            sendingCount++;
        }

        OrderMessage tmOrder = tmSyncQueue.poll();
        if(tmOrder != null){
            _collector.emit(RaceConfig.orderFlowStream,new Values(tmOrder.getOrderId(),RaceConfig.TMORDER, tmOrder.getCreateTime(), tmOrder.getTotalPrice()));
            sendingCount++;
        }

        OrderMessage tbOrder = tbSyncQueue.poll();
        if(tbOrder != null){
            _collector.emit(RaceConfig.orderFlowStream,new Values(tbOrder.getOrderId(),RaceConfig.TBORDER, tbOrder.getCreateTime(), tbOrder.getTotalPrice()));
            sendingCount++;
        }
        updateSendTps();
    }

    @Override
    public void ack(Object id) {
        // Ignored
    }
    @Override
    public void fail(Object id) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(RaceConfig.payStream,new Fields("ctime","platform","amount"));
        declarer.declareStream(RaceConfig.orderFlowStream,new Fields("orderId","from","ctime","amount"));
    }

    private void updateSendTps() {
        if (!isStatEnable)
            return;
        long now = System.currentTimeMillis();
        long interval = now - startTime;
        long count = sendingCount - lastSendingCount;
        if (interval > 3 * 1000) {
            LOG.info("ALL_SPOUT_SENDING_TPS is {} taskId:{}",(count * 1000) / interval,taskId);
            startTime = now;
            lastSendingCount = sendingCount;
        }
    }

    @Override
    public void close() {
        LOG.info("shut down pay push consumer");
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