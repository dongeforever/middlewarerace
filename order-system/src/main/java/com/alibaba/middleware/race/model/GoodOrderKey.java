package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.util.OrderUtil;
import com.alibaba.middleware.race.util.TypeUtil;

import java.util.*;

/**
 * Created by liuzhendong on 16/7/31.
 */
public class GoodOrderKey {

    public byte[] good; //8

    public long orderId; //8
    public long ctime; //8
    public byte amount; //1
    public boolean done;
    public String buyerId; //20
    public byte[] pos;//8


    public static GoodOrderKey parse(byte[] bytes){
        GoodOrderKey goodOrderKey = new GoodOrderKey();
        goodOrderKey.orderId = TypeUtil.bytesToLong(Arrays.copyOfRange(bytes,8,16));
        goodOrderKey.ctime = TypeUtil.bytesToLong(Arrays.copyOfRange(bytes, 16 , 24));
        goodOrderKey.done = (bytes[24] & 0x80) != 0;
        goodOrderKey.amount = (byte) (bytes[24] & 0x7F);
        goodOrderKey.buyerId = new String(bytes,25,20);
        goodOrderKey.pos = Arrays.copyOfRange(bytes,45,53);
        return goodOrderKey;
    }

    private static HashSet<String> indexNames = new HashSet<String>();
    {
        indexNames.add("orderid");
        indexNames.add("createtime");
        indexNames.add("buyerid");
        indexNames.add("goodid");
        indexNames.add("amount");
        indexNames.add("salerid");
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

    public Map<String,String>  filterFieldsInIndex(Collection<String> keys,String goodId,String salerId){
        Map<String,String> fields = new HashMap<String, String>(4);
        for (String key : keys){
            if(key.equals("goodid")){
                fields.put("goodid", goodId);
                continue;
            }
            if(key.equals("salerid")){
                fields.put("salerid",salerId);
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

    public Map<String,String>  filterFieldsInIndexAndGoodOrBuyer(Collection<String> keys,String goodId,String salerId,Map<String,String> good){
        Map<String,String> fields = new HashMap<String, String>(4);
        for (String key : keys){
            if(key.equals("goodid")){
                fields.put("goodid", goodId);
                continue;
            }
            if(key.equals("salerid")){
                fields.put("salerid",salerId);
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

    public Map<String,String>  filterFieldsInIndexAndGoodAndBuyer(Collection<String> keys,String goodId,String salerId,Map<String,String> good,Map<String,String> buyer){
        Map<String,String> fields = new HashMap<String, String>(4);
        for (String key : keys){
            if(key.equals("goodid")){
                fields.put("goodid", goodId);
                continue;
            }
            if(key.equals("salerid")){
                fields.put("salerid",salerId);
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
