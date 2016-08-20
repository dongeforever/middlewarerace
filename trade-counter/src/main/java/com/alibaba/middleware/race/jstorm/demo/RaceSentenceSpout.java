package com.alibaba.middleware.race.jstorm.demo;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.utils.JStormUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

public class RaceSentenceSpout implements IRichSpout {
    private static Logger LOG = LoggerFactory.getLogger(RaceSentenceSpout.class);
    SpoutOutputCollector _collector;
    Random _rand;
    long lastSendingCount;
    long sendingCount;
    long startTime;
    boolean isStatEnable;
    int sendNumPerNexttuple;
    int taskId;

    int ackCount;
    int failCount;

    private static final String[] CHOICES = {"AAAA",
            "BBBB",
            "CCCC",
            "DDDD",
            "EEEE"};

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
        sendingCount = 0;
        lastSendingCount = 0;
        startTime = System.currentTimeMillis();
        sendNumPerNexttuple = JStormUtils.parseInt(conf.get("send.num.each.time"), 1);
        isStatEnable = JStormUtils.parseBoolean(conf.get("is.stat.enable"), false);
        this.taskId = context.getThisTaskId();
        LOG.info("Prepare RaceSentenceSpout taskId{} isStatEnable:{}",taskId, isStatEnable);
    }

    @Override
    public void nextTuple() {
        try {
            String sentence = CHOICES[_rand.nextInt(CHOICES.length)];
            sendingCount++;
            _collector.emit(new Values(sentence),sendingCount);
            LOG.info("SEND_SENTENCE:{} msgId:{}",sentence,sendingCount);
            updateSendTps();
            Thread.sleep(100);
        }catch (Exception e){

        }
    }

    @Override
    public void ack(Object id) {
        LOG.info("RECEIVE_ACK:{}",id);
        ackCount++;
    }

    @Override
    public void fail(Object id) {
        LOG.info("RECEIVE_FAIL:{}",id);
        failCount++;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    private void updateSendTps() {
        if (!isStatEnable)
            return;

        long now = System.currentTimeMillis();
        long interval = now - startTime;
        long count = sendingCount - lastSendingCount;
        if (interval > 5 * 1000) {
            LOG.info("SEND_SENTENCE_TPS of last 5s is {}  ack:{} fail:{} taskId:{}",(count * 1000) / interval,ackCount,failCount,taskId);
            startTime = now;
            lastSendingCount = sendingCount;
        }
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    @Override
    public void activate() {
        // TODO Auto-generated method stub

    }

    @Override
    public void deactivate() {
        // TODO Auto-generated method stub

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}