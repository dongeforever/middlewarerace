package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FlushTairBolt implements IRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(FlushTairBolt.class);

    OutputCollector collector;
    TairOperatorImpl tairOperator;


    int taskId;
    long lastTime;

    @Override
    public void execute(Tuple tuple) {
        int type = tuple.getInteger(0);
        Long key = tuple.getLong(1);
        double value = RaceUtils.round(tuple.getDouble(2));
        LOG.info("FLUSH_TO_TAIR type:{} key:{} value:{}",type, key, value);
        switch (type){
            case  RaceConfig.RATIO:
               tairOperator.write(RaceConfig.prex_ratio+key,value);
               break;
            case RaceConfig.TB_PAY:
               tairOperator.write(RaceConfig.prex_taobao+key,value);
               break;
            case RaceConfig.TM_PAY:
                tairOperator.write(RaceConfig.prex_tmall+key,value);
                break;
            default:
                LOG.info("unknown value type {}",type);
        }
        collector.ack(tuple);
        updateSendTps();

    }

    private void updateSendTps(){
        long now = System.currentTimeMillis();
        long interval = now - lastTime;
        if (interval > 15 * 1000) {
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
        this.tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer,RaceConfig.TairSalveConfigServer,RaceConfig.TairGroup,
                RaceConfig.TairNamespace);
        LOG.info("Prepare FlushTairBolt taskId:{}",taskId);
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub
        tairOperator.close();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}