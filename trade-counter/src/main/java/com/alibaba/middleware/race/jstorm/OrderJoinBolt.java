package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.model.OrderJoinItem;
import com.alibaba.middleware.race.model.OrderJoinMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OrderJoinBolt implements IRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(OrderJoinBolt.class);

    OutputCollector collector;

    OrderJoinMap orderJoinMap;
    int taskId;
    long lastTime;

    @Override
    public void execute(Tuple tuple) {
        collector.ack(tuple);
        Long orderId = tuple.getLong(0);
        int from = tuple.getInteger(1);
        Long ctime = tuple.getLong(2);
        double amount = tuple.getDouble(3);
        //LOG.info("OrderJoinReceive orderId:{} from:{} amount:{}",orderId, from, amount);
        if (from > 0){
            orderJoinMap.merge(new OrderJoinItem(orderId).from(from).totalPrice(amount).ctime(ctime));
        }else {
            orderJoinMap.merge(new OrderJoinItem(orderId).from(from).totalPay(amount).ctime(ctime));
        }
        updateSendTps();

    }

    private void updateSendTps(){
        long now = System.currentTimeMillis();
        long interval = now - lastTime;
        if (interval > 15 * 1000) {
            //LOG.info("Print Word Count taskId:{} map:{}", taskId, counts);
            lastTime = now;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("from","ctime","totalPay"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, final OutputCollector collector) {
        this.collector = collector;
        this.taskId = context.getThisTaskId();
        lastTime = System.currentTimeMillis();
        orderJoinMap = new OrderJoinMap();
        orderJoinMap.joinMeetListener(new OrderJoinMap.JoinMeetListener() {
            @Override
            public void onMeet(OrderJoinItem item) {
                if(item.payItems == null){
                    LOG.error("ON_MEET_ERROR key:{}",item.orderId);
                }
                for (OrderJoinItem payItem: item.payItems){
                    collector.emit(new Values(item.from,payItem.ctime,payItem.totalPay));
                }
            }
        });
        LOG.info("Prepare OrderJoinBolt taskId:{}",taskId);

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