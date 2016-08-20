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
import com.alibaba.middleware.race.model.PayRatio;
import com.alibaba.middleware.race.model.PayRatioArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PayRatioBolt implements IRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(PayRatioBolt.class);

    OutputCollector collector;

    PayRatioArray payRatioArray;
    int taskId;
    long lastTime;
    @Override
    public void execute(Tuple tuple) {
        Long ctime = tuple.getLong(0);
        short platform = tuple.getShort(1);
        double amount = tuple.getDouble(2);
        collector.ack(tuple);
        Long key = RaceUtils.getMinuteTime(ctime);
        //LOG.info("PAT_RATIO_RECEIVE ctime:{} platform:{} amout:{}",ctime,platform,amount);
        if(platform == 0){
            payRatioArray.merge(new PayRatio().key(key).pcPay(amount));
        }else if(platform == 1){
            payRatioArray.merge(new PayRatio().key(key).wxPay(amount));
        }else {
            LOG.info("Pay ratio unknown platform {}",platform);
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

        this.payRatioArray = new PayRatioArray();
        this.payRatioArray.setMoveListener(new PayRatioArray.MoveListener() {
            @Override
            public void onMove(PayRatio payRatio) {
                if(payRatio.key == null) return;
                if(payRatio.pcPay > 0){
                    double ratio = RaceUtils.round(payRatio.wxPay/payRatio.pcPay);
                    collector.emit(new Values(RaceConfig.RATIO, payRatio.key, ratio));
                }
            }
        });
        lastTime = System.currentTimeMillis();
        LOG.info("Prepare PayRatioBolt taskId:{}",taskId);
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub
        payRatioArray.shutdown();

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}