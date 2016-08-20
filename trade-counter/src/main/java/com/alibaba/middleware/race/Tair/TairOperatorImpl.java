package com.alibaba.middleware.race.Tair;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.impl.DefaultTairManager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/**
 * 读写tair所需要的集群信息，如masterConfigServer/slaveConfigServer地址/
 * group 、namespace我们都会在正式提交代码前告知选手
 */
public class TairOperatorImpl {


    DefaultTairManager tairManager = new DefaultTairManager();

    int namespace;

    public TairOperatorImpl(String masterConfigServer,
                            String slaveConfigServer,
                            String groupName,
                            int namespace) {
        List<String> confServers = new ArrayList<String>();
        confServers.add(masterConfigServer);
        confServers.add(slaveConfigServer);
        tairManager.setConfigServerList(confServers);
        tairManager.setGroupName(groupName);
        tairManager.init();
        this.namespace = namespace;
    }

    public boolean write(Serializable key, Serializable value) {
        tairManager.put(namespace,key,value);
        return true;
    }

    public Object get(Serializable key) {
        Result<DataEntry> result = tairManager.get(namespace,key);
        if (result.isSuccess()) {
            DataEntry entry = result.getValue();
            if (entry != null) {
               return entry.getValue();
            } else {
               return null;
            }
        }
        return null;
    }

    public boolean remove(Serializable key) {
        tairManager.delete(namespace, key);
        return true;
    }

    public void close(){
        tairManager.close();
    }

    //天猫的分钟交易额写入tair
    public static void main(String [] args) throws Exception {
        TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
        //假设这是付款时间
        Long millisTime = System.currentTimeMillis();
        //由于整分时间戳是10位数，所以需要转换成整分时间戳
        Long minuteTime = (millisTime / 1000 / 60) * 60;
        //假设这一分钟的交易额是100;
        Double money = 100.0;
        //写入tair
        tairOperator.write(RaceConfig.prex_tmall + minuteTime, money);

        System.out.println(tairOperator.get(RaceConfig.prex_ratio+1467908280));
        System.out.println(tairOperator.get(RaceConfig.prex_taobao+1467932820));
        System.out.println(tairOperator.get(RaceConfig.prex_tmall+1467912060));

    }
}
