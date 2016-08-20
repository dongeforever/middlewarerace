package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OrderFlowBolt implements IRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(OrderFlowBolt.class);

    OutputCollector collector;

    int taskId;
    long lastTime;

    @Override
    public void execute(Tuple tuple) {
        Long orderId = tuple.getLong(0);
        int from = tuple.getInteger(1);
        Long ctime = tuple.getLong(2);
        double amount = tuple.getDouble(3);
        collector.ack(tuple);
        collector.emit(new Values(orderId,from,ctime,amount));
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
        declarer.declare(new Fields("orderId","from","ctime","amount"));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.taskId = context.getThisTaskId();
        lastTime = System.currentTimeMillis();
        LOG.info("Prepare OrderFlowBolt taskId:{}",taskId);
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