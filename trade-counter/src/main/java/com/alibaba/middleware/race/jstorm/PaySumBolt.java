package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.Item;
import com.alibaba.middleware.race.model.ItemMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PaySumBolt implements IRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(PaySumBolt.class);

    OutputCollector collector;

    ItemMap tbPayMap;
    ItemMap tmPayMap;
    int taskId;
    long lastTime;

    @Override
    public void execute(Tuple tuple) {

        int from = tuple.getInteger(0);
        Long ctime = tuple.getLong(1);
        double amount = tuple.getDouble(2);
        collector.ack(tuple);
        if(from == RaceConfig.TBORDER ){
            this.tbPayMap.merge(new Item().amount(amount).key(RaceUtils.getMinuteTime(ctime)));
        }else if (from == RaceConfig.TMORDER){
            this.tmPayMap.merge(new Item().amount(amount).key(RaceUtils.getMinuteTime(ctime)));
        }else {
            LOG.info("unknown from {}",from);
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
        declarer.declare(new Fields("type","key","value"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, final OutputCollector collector) {
        this.collector = collector;
        this.taskId = context.getThisTaskId();
        this.tbPayMap = new ItemMap();
        this.tmPayMap = new ItemMap();
        this.tbPayMap.setRemoveListener(new ItemMap.RemoveListener() {
            @Override
            public void onRemove(Long key, Item item) {
                collector.emit(new Values(RaceConfig.TB_PAY, key, item.amount));
            }
        });
        this.tmPayMap.setRemoveListener(new ItemMap.RemoveListener() {
            @Override
            public void onRemove(Long key, Item item) {
                collector.emit(new Values(RaceConfig.TM_PAY, key, item.amount));
            }
        });
        lastTime = System.currentTimeMillis();
        LOG.info("Prepare PaySumBolt taskId:{}",taskId);
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub
        this.tmPayMap.shutdown();
        this.tbPayMap.shutdown();

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}