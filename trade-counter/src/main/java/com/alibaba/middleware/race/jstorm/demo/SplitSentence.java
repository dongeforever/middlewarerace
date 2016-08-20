package com.alibaba.middleware.race.jstorm.demo;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class SplitSentence implements IRichBolt {
    OutputCollector collector;
    private static Logger LOG = LoggerFactory.getLogger(SplitSentence.class);

    @Override
    public void execute(Tuple tuple) {
        String sentence = tuple.getString(0);
        for (String word : sentence.split("\\s+")) {
            //锚定在这个位置
            collector.emit(tuple,new Values(word));
        }
        LOG.info("SPLIT_SENTENCE:{} msgId:{}",sentence, tuple.getMessageId().getAnchors());
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
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
