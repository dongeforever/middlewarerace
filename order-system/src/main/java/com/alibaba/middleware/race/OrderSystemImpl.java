package com.alibaba.middleware.race;

import com.alibaba.middleware.race.model.Constant;
import com.alibaba.middleware.race.model.GoodOrderKey;
import com.alibaba.middleware.race.model.OrderKey;
import com.alibaba.middleware.race.table.BuyerTable;
import com.alibaba.middleware.race.table.GoodTable;
import com.alibaba.middleware.race.table.OrderTable;
import com.alibaba.middleware.race.util.OrderUtil;
import com.alibaba.middleware.race.util.PrintUtil;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;

/**
 *  交易订单系统接口
 *  CASE:QUERY_BUYER_TSRANGE
 *  CASE:QUERY_GOOD_SUM
 *  CASE:QUERY_ORDER
 *  CASE:QUERY_SALER_GOOD
 *  @author wangxiang@alibaba-inc.com
 */
public class OrderSystemImpl implements OrderSystem{

    public OrderTable orderTable;
    public GoodTable goodTable;
    public BuyerTable buyerTable;

    public final boolean debug = false;
    /**
    * 测试程序调用此接口构建交易订单记录查询系统
    *
    * 输入数据格式请看README.md
    *
    * @param orderFiles
    *          订单文件列表
    * @param buyerFiles
    *          买家文件列表
    * @param goodFiles
    *          商品文件列表
    *
    * @param storeFolders
    *          存储文件夹的目录，保证每个目录是有效的，每个目录在不同的磁盘上
    */
    public void construct(Collection<String> orderFiles,
                        Collection<String> buyerFiles, Collection<String> goodFiles,
                        Collection<String> storeFolders) throws IOException, InterruptedException{

        orderTable = new OrderTable(orderFiles,storeFolders);
        orderTable.buildIndex();
        goodTable = new GoodTable(goodFiles, storeFolders);
        goodTable.buildIndex();
        buyerTable = new BuyerTable(buyerFiles, storeFolders);
        buyerTable.buildIndex();
    }

    public static class BaseKeyValue implements KeyValue{
        String key;
        String value;

        public BaseKeyValue(String key,String value){
          this.key = key;
          this.value = value;
        }
        @Override
        public String key(){
          return this.key;
        }
        @Override
        public String valueAsString(){
          return this.value;
        }
        @Override
        public long valueAsLong() throws TypeException{
          try {
              return Long.valueOf(this.value);
          }catch (Exception e){
              /*
              System.out.println("ParseLone Fail:" + this.key +"-" + this.value);
              try {
                  return Double.valueOf(this.value).longValue();
              }catch (Exception ee){
                  throw new TypeException();
              }*/
              throw new TypeException();
          }
        }
        @Override
        public double valueAsDouble() throws TypeException{
          try {
              return Double.valueOf(this.value);
          }catch (Exception e){
              throw new TypeException();
          }
        }
        @Override
        public boolean valueAsBoolean() throws TypeException{
          try {
              return  Boolean.valueOf(this.value);
          }catch (Exception e){
              throw  new TypeException();
          }
        }

        @Override
        public String toString(){
            return key+":" + value;
        }
    }

    public static class BaseResult implements Result{
        public long orderId;
        public Map<String, String> fields = new HashMap<String, String>();

        public BaseResult(long orderId,Map<String,String> fields){
            this.orderId = orderId;
            this.fields = fields;
        }
        @Override
        public long orderId(){
          return  this.orderId;
        }

        @Override
        public KeyValue get(String key){
              String value = fields.get(key);

                if(value == null){
                    //System.out.println("cannot find key orderid:" + orderId + " key:" + key +" value:" + value);
                    return null;
                }

              return new BaseKeyValue(key,value);
        }

        @Override
        public KeyValue[] getAll(){
          KeyValue[] all = new KeyValue[fields.size()];
          int i = 0;
          for(Map.Entry<String ,String> entry : fields.entrySet()){
              all[i++] = new BaseKeyValue(entry.getKey(),entry.getValue());
          }
          return all;
        }


        @Override
        public boolean equals(Object a){
            if(! (a instanceof BaseResult)) return false;
            BaseResult other = (BaseResult)a;
            if(other.fields.size() != this.fields.size() || other.orderId() != this.orderId()){
                return false;
            }
            for (Map.Entry<String,String> entry : this.fields.entrySet()){
                if(!other.fields.containsKey(entry.getKey())){
                    return false;
                }
                String otherValue = other.fields.get(entry.getKey());
                String value = entry.getValue();
                if(entry.getKey().equals("contactphone")){
                    continue;
                }
                if(!value.equals(otherValue)){
                    return false;
                }
            }
            return true;
        }

        @Override
        public String toString(){
            StringBuilder sb = new StringBuilder();
            sb.append("{").append("orderid:").append(orderId).append(",");
            sb.append("KV:");
            sb.append('[');
            for (Map.Entry<String,String> entry : fields.entrySet()){
                sb.append(entry.getKey()).append(":").append(entry.getValue()).append(',');
            }
            sb.append(']').append('}');
            return sb.toString();
        }
    }
    /**
    * 查询订单号为orderid的指定字段
    *
    * @param orderId
    *          订单号
    * @param keys
    *          待查询的字段，如果为null，则查询所有字段，如果为空，则排除所有字段
    * @return 查询结果，如果该订单不存在，返回null
    */
    public Result queryOrder(final long orderId, Collection<String> keys){
        long start = System.currentTimeMillis();
        try {
            OrderKey orderKey = orderTable.getOrderKey(orderId);
            if(orderKey == null) return null;

            if(keys != null){
                if(OrderKey.onlyInIndex(keys)){
                    long end = System.currentTimeMillis();
                    if(debug) PrintUtil.print("QUERY_ORDER %d %s %d onlyInIndex", orderId, keys, end-start);
                    return new BaseResult(orderId, orderKey.filterFieldsInIndex(keys));
                }
                if(OrderKey.onlyInGoodIndex(keys)){
                    Map<String,String> good = goodTable.getGoodById(orderKey.goodId);
                    long end = System.currentTimeMillis();
                    if(debug) PrintUtil.print("QUERY_ORDER %d %s %d onlyInGoodIndex", orderId, keys, end-start);
                    return new BaseResult(orderId, orderKey.filterFieldsInIndexAndGoodOrBuyer(keys,good));
                }
                if(OrderKey.onlyInIndexAndBuyer(keys)){
                    Map<String,String> buyer = buyerTable.getBuyerById(orderKey.buyerId);
                    long end = System.currentTimeMillis();
                    if(debug) PrintUtil.print("QUERY_ORDER %d %s %d onlyInBuyerIndex", orderId, keys, end-start);
                    return new BaseResult(orderId, orderKey.filterFieldsInIndexAndGoodOrBuyer(keys, buyer));
                }
                if(OrderKey.onlyInIndexAndGoodAndBuyer(keys)){
                    Map<String,String> good = goodTable.getGoodById(orderKey.goodId);
                    Map<String,String> buyer = buyerTable.getBuyerById(orderKey.buyerId);
                    long end = System.currentTimeMillis();
                    if(debug) PrintUtil.print("QUERY_ORDER %d %s %d onlyInGoodAndBuyerIndex", orderId, keys, end-start);
                    return new BaseResult(orderId, orderKey.filterFieldsInIndexAndGoodAndBuyer(keys, good,buyer));
                }
            }
            Map<String,String>  order = orderTable.getOrderByKey(orderKey);
            if(keys!= null && keys.size() == 0){
                return new BaseResult(orderId,new HashMap<String, String>(4));
            }
            Result result= joinOrder(orderId, order,keys,null,null);
            long end = System.currentTimeMillis();
            if(debug) PrintUtil.print("QUERY_ORDER %d %s %d", orderId, keys, end-start);
            return result;

        }catch (Exception e){
            e.printStackTrace();
        }


        return null;
    }

    /**
    * 查询某位买家createtime字段从[startTime, endTime) 时间范围内发生的所有订单的所有信息
    *
    * @param startTime 订单创建时间的下界
    * @param endTime 订单创建时间的上界
    * @param buyerid
    *          买家Id
    * @return 符合条件的订单集合，按照createtime大到小排列
    */
    public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime,
                                      String buyerid){
        long start = System.currentTimeMillis();
        try {
            if(startTime<0) startTime = 0;
            if(endTime < 0) endTime = 0;
            List<Map<String, String>> orders = orderTable.getOrdersByBuyer(buyerid,startTime,endTime);
            //System.out.println("orders:" + orders.size());
            if(orders == null || orders.size() == 0){
                return new ArrayList<Result>().iterator();
            }
            Map<String,String> buyer = buyerTable.getBuyerById(buyerid);
            List<Result>  results = new ArrayList<Result>(orders.size());
            for (int i = orders.size()-1; i >= 0; i--) {
                Map<String,String> order = orders.get(i);
                results.add(joinOrder(Long.valueOf(order.get(Constant.ORDER_ID)),order,null,null,buyer));
            }
            long end = System.currentTimeMillis();
            if(debug) PrintUtil.print("QUERY_BUYER_TSRANGE %s %d %d %d ms",buyerid, startTime, endTime, end - start);
            return results.iterator();
        }catch (Exception e){
            e.printStackTrace();

        }


        return new ArrayList<Result>().iterator();
    }

    /**
    * 查询某位卖家某件商品所有订单的某些字段
    *
    * @param salerid 卖家Id
    * @param goodid 商品Id
    * @param keys 待查询的字段，如果为null，则查询所有字段，如果为空，则排除所有字段
    * @return 符合条件的订单集合，按照订单id从小至大排序
    */
    public Iterator<Result> queryOrdersBySaler(String salerid, String goodid,
                                      Collection<String> keys){
        return _queryOrdersBySaler(salerid, goodid, keys, "QUERY_SALER_GOOD").iterator();
    }

    public List<Result> _queryOrdersBySaler(String salerid, String goodid,
                                               Collection<String> keys, String sign){
        long start = System.currentTimeMillis();

        try {
            List<GoodOrderKey> goodKeys = orderTable.getGoodKeysByGood(goodid);
            if(goodKeys == null || goodKeys.size() == 0){
                return new ArrayList<Result>();
            }

            Map<String,String> good = null;
            if(keys != null){
                if(GoodOrderKey.onlyInIndex(keys)){
                    List<Result>  results = new ArrayList<Result>(goodKeys.size());
                    for (int i = 0; i < goodKeys.size(); i++) {
                        GoodOrderKey tmp = goodKeys.get(i);
                        //TODO 延迟加载
                        results.add(new BaseResult(tmp.orderId, tmp.filterFieldsInIndex(keys,goodid,salerid)));
                    }
                    long end = System.currentTimeMillis();
                    if(debug) PrintUtil.print("%s %s %s %s %d ms only_in_index",sign,salerid, goodid, keys, end - start);
                    return results;
                }
                good = goodTable.getGoodById(goodid);
                if(GoodOrderKey.onlyInGoodIndex(keys)){
                    List<Result>  results = new ArrayList<Result>(goodKeys.size());
                    for (int i = 0; i < goodKeys.size(); i++) {
                        GoodOrderKey tmp = goodKeys.get(i);
                        results.add(new BaseResult(tmp.orderId, tmp.filterFieldsInIndexAndGoodOrBuyer(keys,goodid,salerid,good)));
                    }
                    long end = System.currentTimeMillis();
                    if(debug) PrintUtil.print("%s %s %s %s %d ms only_in_good_index",sign,salerid, goodid, keys, end - start);
                    return results;
                }

                if(GoodOrderKey.onlyInIndexAndBuyer(keys)){
                    List<Result>  results = new ArrayList<Result>(goodKeys.size());
                    for (int i = 0; i < goodKeys.size(); i++) {
                        GoodOrderKey tmp = goodKeys.get(i);
                        Map<String,String> buyer = buyerTable.getBuyerById(tmp.buyerId);
                        results.add(new BaseResult(tmp.orderId, tmp.filterFieldsInIndexAndGoodOrBuyer(keys,goodid,salerid,buyer)));
                    }
                    long end = System.currentTimeMillis();
                    if(debug) PrintUtil.print("%s %s %s %s %d ms only_in_buyer_index",sign,salerid, goodid, keys, end - start);
                    return results;
                }
                if(GoodOrderKey.onlyInIndexAndGoodAndBuyer(keys)){
                    List<Result>  results = new ArrayList<Result>(goodKeys.size());
                    for (int i = 0; i < goodKeys.size(); i++) {
                        GoodOrderKey tmp = goodKeys.get(i);
                        Map<String,String> buyer = buyerTable.getBuyerById(tmp.buyerId);
                        results.add(new BaseResult(tmp.orderId, tmp.filterFieldsInIndexAndGoodAndBuyer(keys,goodid,salerid,good,buyer)));
                    }
                    long end = System.currentTimeMillis();
                    if(debug) PrintUtil.print("%s %s %s %s %d ms only_in_buyer_and_good_index",sign,salerid, goodid, keys, end - start);
                    return results;
                }
            }
            List<Map<String, String>> orders = orderTable.getOrderByKeys(goodKeys);
            List<Result>  results = joinOrders(orders, keys, good, null);
            long end = System.currentTimeMillis();
            if(debug) PrintUtil.print("%s %s %s %s %d ms",sign,salerid, goodid, keys, end - start);
            return results;
        }catch (Exception e){
            e.printStackTrace();
        }

        return new ArrayList<Result>();
    }
    /**
    * 对某件商品的某个字段求和，只允许对long和double类型的KV求和 如果字段中既有long又有double，则使用double
    * 如果求和的key中包含非long/double类型字段，则返回null 如果查询订单中的所有商品均不包含该字段，则返回null
    *
    * @param goodid 商品Id
    * @param key 求和字段
    * @return 求和结果
    */
    public KeyValue sumOrdersByGood(String goodid, String key){
        long start = System.currentTimeMillis();
        try {
            List<String> keys = new ArrayList<String>(4);
            keys.add(key);
            List<Result>  results = _queryOrdersBySaler("",goodid,keys,"SUM_QUERY_SALER_GOOD");
            if(results.size() == 0) return null;
            //先假设为long
            {
                Long value = 0l;
                boolean allNull = true;
                boolean correct = true;
                for (Result result : results) {
                    if (result.get(key) == null || result.get(key).valueAsString() == null) {
                        continue;
                    }
                    allNull = false;
                    try {
                        value += Long.valueOf(result.get(key).valueAsString());
                    } catch (Exception e) {
                        correct = false;
                        break;
                    }
                }
                if(correct){
                    long end = System.currentTimeMillis();
                    if(debug) PrintUtil.print("QUERY_GOOD_SUM %s %s %d ms", goodid, key, end - start);
                    if (allNull) return null;
                    return new BaseKeyValue(key, value.toString());
                }
            }
            {
                Double value = 0.0d;
                boolean allNull = true;
                for (Result result : results) {
                    if (result.get(key) == null || result.get(key).valueAsString() == null) {
                        continue;
                    }
                    allNull = false;
                    try {
                        value += result.get(key).valueAsDouble();
                    } catch (TypeException e) {
                        e.printStackTrace();
                        return null;
                    }
                }
                long end = System.currentTimeMillis();
                if(debug) PrintUtil.print("QUERY_GOOD_SUM %s %s %d ms", goodid, key, end - start);
                if (allNull) return null;
                return new BaseKeyValue(key, value.toString());
            }
        }catch (Exception e){
            e.printStackTrace();
        }


        return null;
    }


    private HashSet<String>  orderKeys = new HashSet<String>();
    {
        orderKeys.add("orderid");
        orderKeys.add("createtime");
        orderKeys.add("buyerid");
        orderKeys.add("goodid");
        orderKeys.add("amount");
        orderKeys.add("done");
        orderKeys.add("remark");
    }

    private HashSet<String>  goodKeys = new HashSet<String>();
    {
        goodKeys.add("goodid");
        goodKeys.add("salerid");
        goodKeys.add("good_name");
        goodKeys.add("description");
        goodKeys.add("price");
        goodKeys.add("offprice");
    }

    private HashSet<String>  buyerKeys = new HashSet<String>();
    {
        buyerKeys.add("buyerid");
        buyerKeys.add("contactphone");
        buyerKeys.add("address");
        buyerKeys.add("buyername");
    }
    public boolean onlyInOrders(Collection<String> keys){
        for (String key : keys){
            if(orderKeys.contains(key) || key.startsWith("a_o_")){

            }else {
                return false;
            }
        }
        return true;
    }

    public boolean onlyInGoods(Collection<String> keys){
        for (String key : keys){
            if(goodKeys.contains(key) || key.startsWith("a_g_")){

            }else {
                return false;
            }
        }
        return true;
    }
    public boolean onlyInBuyers(Collection<String> keys){
        for (String key : keys){
            if(buyerKeys.contains(key) || key.startsWith("a_b_")){

            }else {
                return false;
            }
        }
        return true;
    }

    public boolean onlyInOrderGoods(Collection<String> keys){
        for (String key : keys){
            if(goodKeys.contains(key) || key.startsWith("a_g_") || orderKeys.contains(key) || key.startsWith("a_o_")){

            }else {
                return false;
            }
        }
        return true;
    }

    public boolean onlyInOrderBuyer(Collection<String> keys){
        for (String key : keys){
            if(buyerKeys.contains(key) || key.startsWith("a_b_") || orderKeys.contains(key) || key.startsWith("a_o_")){

            }else {
                return false;
            }
        }
        return true;
    }
    private List<Result> joinOrders(List<Map<String,String>> orders, Collection<String> keys, Map<String,String> good,Map<String,String> buyer)throws Exception{
        //优化的基本思路就是提前确定 keys多落在哪些表里面
        if(keys == null){
            List<Result>  results = new ArrayList<Result>();
            for (Map<String, String> order : orders){
                results.add(joinOrder(Long.valueOf(order.get(Constant.ORDER_ID)),order,keys,good,buyer));
            }
            return results;
        }

        if(onlyInOrders(keys)){
            List<Result>  results = new ArrayList<Result>();
            for (Map<String, String> order : orders){
                Map<String,String> fields = new HashMap<String, String>(keys.size());
                OrderUtil.filter(fields,order,keys);
                results.add(new BaseResult(Long.valueOf(order.get(Constant.ORDER_ID)),fields));
            }
            return results;
        }

        if(onlyInGoods(keys)){
            List<Result>  results = new ArrayList<Result>();
            for (Map<String, String> order : orders){
                Map<String,String> fields = new HashMap<String, String>(keys.size());
                OrderUtil.filter(fields, good != null ? good : goodTable.getGoodById(order.get(Constant.GOOD_ID)),keys);
                results.add(new BaseResult(Long.valueOf(order.get(Constant.ORDER_ID)),fields));
            }
            return results;
        }
        if(onlyInBuyers(keys)){
            List<Result>  results = new ArrayList<Result>();
            for (Map<String, String> order : orders){
                Map<String,String> fields = new HashMap<String, String>(keys.size());
                OrderUtil.filter(fields, buyer != null ? buyer : buyerTable.getBuyerById(order.get(Constant.BUYER_ID)),keys);
                results.add(new BaseResult(Long.valueOf(order.get(Constant.ORDER_ID)),fields));
            }
            return results;
        }

        if(onlyInOrderGoods(keys)){
            List<Result>  results = new ArrayList<Result>();
            for (Map<String, String> order : orders){
                results.add(joinOrderWithGood(Long.valueOf(order.get(Constant.ORDER_ID)),order,keys,good));
            }
            return results;
        }
        if(onlyInOrderBuyer(keys)){
            List<Result>  results = new ArrayList<Result>();
            for (Map<String, String> order : orders){
                results.add(joinOrderWithBuyer(Long.valueOf(order.get(Constant.ORDER_ID)),order,keys,buyer));
            }
            return results;
        }
        List<Result>  results = new ArrayList<Result>();
        for (Map<String, String> order : orders){
            results.add(joinOrder(Long.valueOf(order.get(Constant.ORDER_ID)),order,keys,good,null));
        }
        return results;

    }


    private Result joinOrder(long orderId,Map<String,String> order, Collection<String> keys,Map<String,String> good,Map<String,String> buyer)throws Exception{

        if(keys == null){
            Map<String,String> fields = new HashMap<String, String>(32);
            fields.putAll(order);
            fields.putAll(good != null ? good : goodTable.getGoodById(order.get(Constant.GOOD_ID)));
            fields.putAll(buyer != null ? buyer :buyerTable.getBuyerById(order.get(Constant.BUYER_ID)));
            return new BaseResult(orderId, fields);
        }
        if(onlyInOrders(keys)){
            Map<String,String> fields = new HashMap<String, String>(keys.size());
            OrderUtil.filter(fields,order,keys);
            return new BaseResult(orderId,fields);
        }
        Map<String,String> fields = new HashMap<String, String>(keys.size());
        OrderUtil.filter(fields,order,keys);
        if(fields.size() == keys.size()){
            return new BaseResult(orderId,fields);
        }
        OrderUtil.filter(fields, good != null ? good : goodTable.getGoodById(order.get(Constant.GOOD_ID)),keys);
        if(fields.size() == keys.size()){
            return new BaseResult(orderId,fields);
        }
        OrderUtil.filter(fields, buyer != null ? buyer : buyerTable.getBuyerById(order.get(Constant.BUYER_ID)),keys);
        return new BaseResult(orderId,fields);
    }


    private Result joinOrderWithGood(long orderId,Map<String,String> order, Collection<String> keys,Map<String,String> good)throws Exception{

        Map<String,String> fields = new HashMap<String, String>(keys.size());
        OrderUtil.filter(fields,order,keys);
        if(fields.size() == keys.size()){
            return new BaseResult(orderId,fields);
        }
        OrderUtil.filter(fields, good != null ? good : goodTable.getGoodById(order.get(Constant.GOOD_ID)),keys);
        if(fields.size() == keys.size()){
            return new BaseResult(orderId,fields);
        }
        return new BaseResult(orderId,fields);
    }
    private Result joinOrderWithBuyer(long orderId,Map<String,String> order, Collection<String> keys, Map<String,String> buyer)throws Exception{

        Map<String,String> fields = new HashMap<String, String>(keys.size());
        OrderUtil.filter(fields,order,keys);
        if(fields.size() == keys.size()){
            return new BaseResult(orderId,fields);
        }
        OrderUtil.filter(fields, buyer != null ? buyer : buyerTable.getBuyerById(order.get(Constant.BUYER_ID)),keys);
        return new BaseResult(orderId,fields);
    }

}
