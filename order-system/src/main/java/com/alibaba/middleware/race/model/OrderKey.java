package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.util.OrderUtil;
import com.alibaba.middleware.race.util.TypeUtil;

import java.util.*;

/**
 * Created by liuzhendong on 16/7/31.
 */
public class OrderKey {


    public long orderId; //8
    public long ctime; //8
    public byte amount; //1
    public boolean done;
    public String buyerId; //20
    public String goodId;//21
    public byte[] pos;//8


    public static OrderKey parse(byte[] bytes){
        OrderKey orderKey = new OrderKey();
        orderKey.orderId = TypeUtil.bytesToLong(Arrays.copyOfRange(bytes,0,8));
        orderKey.ctime = TypeUtil.bytesToLong(Arrays.copyOfRange(bytes, 8 , 16));
        orderKey.done = (bytes[16] & 0x80) != 0;
        orderKey.amount = (byte) (bytes[16] & 0x7F);
        orderKey.buyerId = new String(bytes,17,20);
        orderKey.goodId = new String(bytes, 37, 21).trim();
        orderKey.pos = Arrays.copyOfRange(bytes,58, 66);
        return orderKey;
    }

    private static HashSet<String> indexNames = new HashSet<String>();
    {
        indexNames.add("orderid");
        indexNames.add("createtime");
        indexNames.add("amount");
        indexNames.add("goodid");
        indexNames.add("buyerid");
        indexNames.add("done");

    }

    private static HashSet<String>  goodKeys = new HashSet<String>();
    {
        goodKeys.add("goodid");
        goodKeys.add("salerid");
        goodKeys.add("good_name");
        goodKeys.add("description");
        goodKeys.add("price");
        goodKeys.add("offprice");
    }
    private static HashSet<String>  buyerKeys = new HashSet<String>();
    {
        buyerKeys.add("buyerid");
        buyerKeys.add("contactphone");
        buyerKeys.add("address");
        buyerKeys.add("buyername");
    }
    public static boolean onlyInIndex(Collection<String> keys){
        for (String key : keys){
            if(indexNames.contains(key)){

            }else {
                return false;
            }
        }
        return true;
    }
    public static  boolean onlyInGoodIndex(Collection<String> keys){
        for (String key : keys){
            if(indexNames.contains(key) || goodKeys.contains(key) || key.startsWith("a_g_")){

            }else {
                return false;
            }
        }
        return true;
    }

    public static  boolean onlyInIndexAndBuyer(Collection<String> keys){
        for (String key : keys){
            if(indexNames.contains(key) || buyerKeys.contains(key) || key.startsWith("a_b_")){

            }else {
                return false;
            }
        }
        return true;
    }

    public static boolean onlyInIndexAndGoodAndBuyer(Collection<String> keys){
        for (String key : keys){
            if(indexNames.contains(key) || buyerKeys.contains(key) || key.startsWith("a_b_")
                    || goodKeys.contains(key) || key.startsWith("a_g_")){

            }else {
                return false;
            }
        }
        return true;
    }

    public Map<String,String>  filterFieldsInIndex(Collection<String> keys){
        Map<String,String> fields = new HashMap<String, String>(4);
        for (String key : keys){
            if(key.equals("goodid")){
                fields.put("goodid", goodId);
                continue;
            }
            if(key.equals("orderid")){
                fields.put("orderid",orderId+"");
                continue;
            }
            if(key.equals("createtime")){
                fields.put("createtime", ctime+"");
                continue;
            }
            if(key.equals("buyerid")){
                fields.put("buyerid", buyerId);
                continue;
            }
            if(key.equals("amount")){
                fields.put("amount", amount+"");
                continue;
            }
            if(key.equals("done")){
                fields.put("done", String.valueOf(done));
                continue;
            }
        }
        return fields;
    }

    public Map<String,String>  filterFieldsInIndexAndGoodOrBuyer(Collection<String> keys,Map<String,String> good){
        Map<String,String> fields = new HashMap<String, String>(4);
        for (String key : keys){
            if(key.equals("goodid")){
                fields.put("goodid", goodId);
                continue;
            }
            if(key.equals("orderid")){
                fields.put("orderid",orderId+"");
                continue;
            }
            if(key.equals("createtime")){
                fields.put("createtime", ctime+"");
                continue;
            }
            if(key.equals("buyerid")){
                fields.put("buyerid", buyerId);
                continue;
            }
            if(key.equals("amount")){
                fields.put("amount", amount+"");
                continue;
            }
            if(key.equals("done")){
                fields.put("done", String.valueOf(done));
                continue;
            }
        }
        OrderUtil.filter(fields, good, keys);
        return fields;
    }

    public Map<String,String>  filterFieldsInIndexAndGoodAndBuyer(Collection<String> keys,Map<String,String> good,Map<String,String> buyer){
        Map<String,String> fields = new HashMap<String, String>(4);
        for (String key : keys){
            if(key.equals("goodid")){
                fields.put("goodid", goodId);
                continue;
            }
            if(key.equals("orderid")){
                fields.put("orderid",orderId+"");
                continue;
            }
            if(key.equals("createtime")){
                fields.put("createtime", ctime+"");
                continue;
            }
            if(key.equals("buyerid")){
                fields.put("buyerid", buyerId);
                continue;
            }
            if(key.equals("amount")){
                fields.put("amount", amount+"");
                continue;
            }
            if(key.equals("done")){
                fields.put("done", String.valueOf(done));
                continue;
            }
        }
        OrderUtil.filter(fields, good, keys);
        OrderUtil.filter(fields, buyer, keys);
        return fields;
    }
}
