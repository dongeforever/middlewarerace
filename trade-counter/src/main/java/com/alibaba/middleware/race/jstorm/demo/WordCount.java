package com.alibaba.middleware.race.jstorm.demo;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.jstorm.demo.RaceSentenceSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WordCount implements IRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(RaceSentenceSpout.class);

    OutputCollector collector;
    Map<String, Integer> counts = new ConcurrentHashMap<String, Integer>();

    int taskId;
    long lastTime;

    int receiveNum;
    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        Integer count = counts.get(word);
        if (count == null)
            count = 0;
        counts.put(word, ++count);
        LOG.info("COUNT_WORD:{}",tuple.getMessageId().getAnchors());
        collector.ack(tuple);
        receiveNum++;
        updateSendTps();

    }

    private void updateSendTps(){
        long now = System.currentTimeMillis();
        long interval = now - lastTime;
        if (interval > 5 * 1000) {
            LOG.info("Print Word Count taskId:{} size:{}", taskId, counts.size());
            counts.clear();
            lastTime = now;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.taskId = context.getThisTaskId();
        lastTime = System.currentTimeMillis();

        LOG.info("Prepare WordCount taskId{}",taskId);
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