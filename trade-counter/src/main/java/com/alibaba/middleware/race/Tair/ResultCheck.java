package com.alibaba.middleware.race.Tair;

import com.alibaba.middleware.race.RaceConfig;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * Created by liuzhendong on 16/7/9.
 */
public class ResultCheck {

    public static void main(String [] args) throws Exception {
        if(args.length != 1){
            System.out.println("usage:$0 filePath");
            System.exit(1);
        }
        TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
        Properties properties = new Properties();
        properties.load(new FileInputStream(args[0]));
        int allNum=0,errNum=0;
        for (Object object : properties.keySet()){
            String key = (String)object;
            Double value = Double.valueOf((String)properties.get(key));
            Double tairValue = Double.valueOf(String.valueOf(tairOperator.get(key)));
            allNum++;
            if(tairValue != value){
                errNum++;
                System.out.println(String.format("value:%f tairValue:%f",value, tairValue));
            }
        }
        System.out.println(String.format("allNum:%d errNum:%d",allNum, errNum));
    }
}
