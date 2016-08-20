package com.alibaba.middleware.race;

import com.alibaba.middleware.race.util.OrderUtil;
import com.alibaba.middleware.race.util.PrintUtil;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by liuzhendong on 16/7/28.
 */
public class Tester {

    public static void test(OrderSystem orderSystem)throws Exception{
        String file = "/Users/liuzhendong/Source/middleware-race/data/prerun_data/case.0";
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        String line = null;
        int i = 0,max =10000;
        int salerSuccNum=0,salerFailNum=0;
        int goodSumSucc=0,goodSumFail=0;
        int buerTsSucc = 0, buyerTsFail = 0;
        int queryOrderSucc = 0, queryOrderFail = 0;
        long start = System.currentTimeMillis();
        while ((line = br.readLine()) != null){

            if(line.equals("CASE:QUERY_SALER_GOOD")){
                if(parseSalerGood(br, orderSystem)){
                    salerSuccNum++;
                }else {
                    salerSuccNum++;
                }
                if(++i >= max){
                    break;
                }
                if(salerFailNum > 10) break;

                continue;
            }


            if(line.equals("CASE:QUERY_GOOD_SUM")){
                if(parseGoodSum(br, orderSystem)){
                    goodSumSucc++;
                }else {
                    goodSumFail++;
                }
                if(++i > max){
                    break;
                }
                if(goodSumFail > 10) break;
                continue;
            }


            if(line.equals("CASE:QUERY_BUYER_TSRANGE")){
                if(parseBuyerTsRange(br, orderSystem)){
                    buerTsSucc++;
                }else {
                    buyerTsFail++;
                }
                if(++i > max){
                    break;
                }
                if(buyerTsFail > 10) break;
                continue;
            }

            if(line.equals("CASE:QUERY_ORDER")){
                if(parseOrder(br, orderSystem)){
                    queryOrderSucc++;
                }else {
                    queryOrderFail++;
                }
                if(++i > max){
                    break;
                }
                if(queryOrderFail > 10) break;
                continue;
            }

        }
        long end = System.currentTimeMillis();
        synchronized (Tester.class){
            System.out.println(Thread.currentThread().getId()+"-start=======================================================");
            System.out.println(String.format("QUERY_ORDER succNum:%d failNum:%d cost:%d", queryOrderSucc, queryOrderFail, end -  start));
            System.out.println(String.format("QUERY_BUYER_TSRANGE succNum:%d failNum:%d cost:%d", buerTsSucc, buyerTsFail, end -  start));
            System.out.println(String.format("QUERY_SALER_GOOD succNum:%d failNum:%d cost:%d", salerSuccNum, salerFailNum, end -  start));
            System.out.println(String.format("QUERY_GOOD_SUM succNum:%d failNum:%d cost:%d", goodSumSucc, goodSumFail, end -  start));
            System.out.println(Thread.currentThread().getId()+"-end=======================================================");
            PrintUtil.print("rank cost:" + ((OrderSystemImpl)orderSystem).orderTable.rankCost);
        }
    }


    public static boolean parseSalerGood(BufferedReader br,OrderSystem orderSystem)throws Exception{
        String salerId = br.readLine().split(":")[1];
        String goodId = br.readLine().split(":")[1];
        String keyLine  = br.readLine();
        List<String> keys = parseKeys(keyLine);
        br.readLine(); //Result:{
        List<OrderSystem.Result> results = new ArrayList<OrderSystem.Result>();
        String resLine = null;
        while (!(resLine = br.readLine()).equals("}")){
            results.add(parseResult(resLine));
        }
        //System.out.println("====================================================================");
        //PrintUtil.print("START QUERY_SALER_GOOD goodid:%s keys:%s", goodId, keys);
        Iterator<OrderSystem.Result> realRes = orderSystem.queryOrdersBySaler(salerId,goodId,keys);
        boolean succ = true;
        int num = 0;
        while (realRes.hasNext()){
            OrderSystem.Result next = realRes.next();
            if(next.equals(results.get(num))){
            }else {
                System.out.println(next +" vs " + results.get(num));
                succ = false;
                break;
            }
            num++;
        }
        if(num != results.size()){
            System.out.println(String.format("%d != %d",num, results.size()-1));
            succ = false;
        }
        if(succ){
            //System.out.println(String.format("SUCC QUERY_SALER_GOOD goodid:%s keys:%s", goodId, keys));
        }else {
            System.out.println(String.format("FAIL QUERY_SALER_GOOD goodid:%s keys:%s", goodId, keys));
        }
        //System.out.println("====================================================================");
        return succ;
    }


    public static boolean parseOrder(BufferedReader br,OrderSystem orderSystem)throws Exception{
        Long orderId = Long.valueOf(br.readLine().split(":")[1]);
        String keyLine  = br.readLine();
        List<String> keys = parseKeys(keyLine);
        br.readLine(); //Result:{
        List<OrderSystem.Result> results = new ArrayList<OrderSystem.Result>();
        String resLine = null;
        while (!(resLine = br.readLine()).equals("}")){
            results.add(parseResult(resLine));
        }
        //System.out.println("====================================================================");
        //PrintUtil.print("START QUERY_ORDER orderId:%d keys:%s", orderId, keys);
        OrderSystem.Result realRes = orderSystem.queryOrder(orderId, keys);
        OrderSystem.Result expect = results.size() > 0 ? results.get(0) : null;
        boolean succ = true;
        if(expect == null && realRes == null){
            succ = true;
        }else {
            succ = realRes.equals(expect);
        }
        if(succ){
            //PrintUtil.print("SUCC QUERY_ORDER orderId:%d keys:%s", orderId, keys);
        }else {
            PrintUtil.print("FAIL QUERY_ORDER orderId:%d keys:%s", orderId, keys);
        }
        //System.out.println("====================================================================");
        return succ;
    }

    public static boolean parseBuyerTsRange(BufferedReader br,OrderSystem orderSystem)throws Exception{
        String buyerId = br.readLine().split(":")[1];
        Long startTime = Long.valueOf(br.readLine().split(":")[1]);
        Long endTime = Long.valueOf(br.readLine().split(":")[1]);
        br.readLine(); //Result:{
        List<OrderSystem.Result> results = new ArrayList<OrderSystem.Result>();
        String resLine = null;
        while (!(resLine = br.readLine()).equals("}")){
            results.add(parseResult(resLine));
        }
        //System.out.println("====================================================================");
        //PrintUtil.print("START QUERY_BUYER_TSRANGE buyerId:%s start:%d end:%d" , buyerId, startTime, endTime);
        Iterator<OrderSystem.Result> realRes = orderSystem.queryOrdersByBuyer(startTime, endTime, buyerId);
        boolean succ = true;
        int num = 0;
        while (realRes.hasNext()){
            OrderSystem.Result next = realRes.next();
            if(next.equals(results.get(num))){
            }else {
                System.out.println(next +" vs " + results.get(num));
                succ = false;
                break;
            }
            num++;
        }
        if(num != results.size()){
            System.out.println(String.format("%d != %d",num, results.size()-1));
            succ = false;
        }
        if(succ){
            //PrintUtil.print("SUCC QUERY_BUYER_TSRANGE buyerId:%s start:%d end:%d" , buyerId, startTime, endTime);
        }else {
            PrintUtil.print("FAIL QUERY_BUYER_TSRANGE buyerId:%s start:%d end:%d" , buyerId, startTime, endTime);

        }
        //System.out.println("====================================================================");
        return succ;
    }

    public static boolean parseGoodSum(BufferedReader br,OrderSystem orderSystem)throws Exception{
        String goodId = br.readLine().split(":")[1];
        String keyLine  = br.readLine();
        List<String> keys = parseKeys(keyLine);
        String result = br.readLine().split(":")[1];

        //System.out.println("====================================================================");
        //PrintUtil.print("START QUERY_GOOD_SUM goodid:%s keys:%s result:%s", goodId, keys, result);
        OrderSystem.KeyValue realRes = orderSystem.sumOrdersByGood(goodId,keys.get(0));
        boolean succ = true;
        int num = 0;
        if(result.equals("null") && realRes == null){
            succ = true;
        }else {
            try {
                Long expect = Long.valueOf(result);
                return expect.equals(realRes.valueAsLong());
            }catch (Exception e){
                Double expect = Double.valueOf(result);
                if(expect - realRes.valueAsDouble() < 0.0001){
                    succ = true;
                }
            }
        }
        if(succ){
            //System.out.println(String.format("SUCC QUERY_SALER_GOOD goodid:%s keys:%s", goodId, keys));
        }else {
            System.out.println(String.format("FAIL QUERY_SUM_GOOD goodid:%s keys:%s", goodId, keys.get(0)));
        }
        //System.out.println("====================================================================");
        return succ;
    }

    public static OrderSystem.Result parseResult(String line){
        line = line.substring(1,line.length()-1);
        Long orderId = Long.valueOf(line.split(",")[0].split(":")[1]);
        String kvStr = line.split(", KV:")[1];
        kvStr = kvStr.substring(1,kvStr.length()-1);
        Map<String,String> fields = parseFields(kvStr);
        return new OrderSystemImpl.BaseResult(orderId,fields);
    }

    public static Map<String,String> parseFields(String line){

        String[]  splits = line.split(",");
        Map<String,String> fields = new HashMap<String, String>();
        for (String split:splits){
            if(split.length() == 0) continue;
            int index = split.indexOf((int)':');
            if(index == -1) continue;
            fields.put(split.substring(0,index),split.substring(index+1));
        }
        return fields;
    }
    public static List<String> parseKeys(String line){
        line = line.split(":")[1];
        String[] items  = line.substring(1,line.length()-1).split(",");
        if(items[0].equals("*")) return null;
        List<String> keys = new ArrayList<String>();
        for (String item : items){
            if(item.length() > 0){
                keys.add(item);
            }
        }
        return keys;

    }
}
