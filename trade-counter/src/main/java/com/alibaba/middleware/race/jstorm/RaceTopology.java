package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.demo.RaceSentenceSpout;
import com.alibaba.middleware.race.jstorm.demo.SplitSentence;
import com.alibaba.middleware.race.jstorm.demo.WordCount;
import com.alibaba.middleware.race.jstorm.v3.BatchSpout;
import com.alibaba.middleware.race.jstorm.v3.CalculateBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


/**
 * 这是一个很简单的例子
 * 选手的拓扑提交到集群，我们是有超时设置的。每个选手的拓扑最多跑20分钟，一旦超过这个时间
 * 我们会将选手拓扑杀掉。
 */

/**
 * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
 * 因为我们后台对选手的git进行下载打包，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.RaceTopology；
 * 所以这个主类路径一定要正确
 */
public class RaceTopology {

    private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);


    public static void submitLocal(String topologyName,Map conf, StormTopology stormTopology)throws Exception{
        LOG.info("Submit Local");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, stormTopology);

        Thread.sleep(60000);
        cluster.killTopology(topologyName);
        cluster.shutdown();
        LOG.info("shut down local cluster");
    }


    public static void  race(boolean local){
        Config conf = new Config();
        conf.put("is.stat.enable",false);


        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("all_spout", new AllSpout(), 1);

        //builder.setSpout("pay_spout", new PaySpout(), 1);
        //builder.setSpout("tborder_spout", new TBSpout(), 1);
        //builder.setSpout("tmorder_spout", new TMSpout(), 1);

        //合流,按 orderId再分流join
        //builder.setBolt("order_flow_bolt", new OrderFlowBolt(), 1).shuffleGrouping("pay_spout", RaceConfig.orderFlowStream)
        //        .shuffleGrouping("tborder_spout").shuffleGrouping("tmorder_spout");
        builder.setBolt("order_join_bolt", new OrderJoinBolt(), 1).fieldsGrouping("all_spout", RaceConfig.orderFlowStream, new Fields("orderId"));
        //计算tb和tm各自的累计值
        builder.setBolt("pay_sum_bolt", new PaySumBolt(), 1).shuffleGrouping("order_join_bolt");


        //单独计算ratio,按时刻表顺序累计
        builder.setBolt("pay_ratio_bolt", new PayRatioBolt(), 1).shuffleGrouping("all_spout", RaceConfig.payStream);

        //写入tair
        BoltDeclarer flushBolt = builder.setBolt("flush_tair_bolt", new FlushTairBolt(),1);
        flushBolt.shuffleGrouping("pay_ratio_bolt");
        flushBolt.shuffleGrouping("pay_sum_bolt");
        String topologyName = RaceConfig.JstormTopologyName;

        try {
            if(local){
                submitLocal(topologyName,conf,builder.createTopology());
            }else {
                StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    public static void  allInOneRace(boolean local){
        Config conf = new Config();
        conf.put("is.stat.enable",false);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("all_in_one_spout", new AllInOneSpout(), 1);
        //写入tair
        BoltDeclarer flushBolt = builder.setBolt("flush_tair_bolt", new FlushTairBolt(),1);
        flushBolt.shuffleGrouping("all_in_one_spout");
        String topologyName = RaceConfig.JstormTopologyName;
        try {
            if(local){
                submitLocal(topologyName,conf,builder.createTopology());
            }else {
                StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    public static void  allInOneRaceV2(boolean local){
        Config conf = new Config();
        conf.put("is.stat.enable",false);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("all_in_one_spout_2", new AllInOneSpoutV2(), 1);
        //写入tair
        BoltDeclarer flushBolt = builder.setBolt("flush_tair_bolt", new FlushTairBolt(),1);
        flushBolt.shuffleGrouping("all_in_one_spout_2");
        String topologyName = RaceConfig.JstormTopologyName;
        try {
            if(local){
                submitLocal(topologyName,conf,builder.createTopology());
            }else {
                StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void  raceV3(boolean local){
        Config conf = new Config();
        conf.put("is.stat.enable",false);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("batch_spout", new BatchSpout(), 1);
        builder.setBolt("calculate_bolt", new CalculateBolt(),1).shuffleGrouping("batch_spout");
        //写入tair
        builder.setBolt("flush_tair_bolt", new FlushTairBolt(),1).shuffleGrouping("calculate_bolt");
        String topologyName = RaceConfig.JstormTopologyName;
        try {
            if(local){
                submitLocal(topologyName,conf,builder.createTopology());
            }else {
                StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        //race(false);
        //allInOneRace(false);
        allInOneRaceV2(false);
        //raceV3(false);
    }
}