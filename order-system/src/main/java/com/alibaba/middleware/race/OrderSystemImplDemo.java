package com.alibaba.middleware.race;

import com.alibaba.middleware.race.model.Constant;
import com.alibaba.middleware.race.util.OrderUtil;

import java.io.*;
import java.util.*;

/**
 *  交易订单系统接口
 *  CASE:QUERY_BUYER_TSRANGE
 *  CASE:QUERY_GOOD_SUM
 *  CASE:QUERY_ORDER
 *  CASE:QUERY_SALER_GOOD
 *  @author wangxiang@alibaba-inc.com
 */
public class OrderSystemImplDemo implements OrderSystem{

    List<File>  _orderFiles;
    List<File>  _buyerFiles;
    List<File>  _goodFiles;
    List<RandomAccessFile> _orderAccessList;
    List<RandomAccessFile> _buyerAccessList;
    List<RandomAccessFile> _goodAccessList;


    /*
    //正式用
    BTree buyerIdTree;
    BTree goodIdTree;
    BTree orderIdTree;
    BTree orderByBuyerTree;
    BTree orderByGoodTree;
    */

    //先在内存中测试一下
    Map<String,Map<String,String>> buyerIdMap;
    Map<String,Map<String,String>> goodIdMap;
    Map<Long,Map<String,String>> orderIdMap;
    Map<String,List<Long>> orderByBuyerMap;
    Map<String,List<Long>> orderByGoodMap;

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
        //测试创建索引文件
        RandomAccessFile randomAccessFile = new RandomAccessFile(storeFolders.iterator().next()+"/orderIndex","rw");
        
        _orderFiles = new ArrayList<File>(orderFiles.size());
        _buyerFiles = new ArrayList<File>(buyerFiles.size());
        _goodFiles = new ArrayList<File>(goodFiles.size());
        _orderAccessList = new ArrayList<RandomAccessFile>(orderFiles.size());
        _buyerAccessList = new ArrayList<RandomAccessFile>(buyerFiles.size());
        _goodAccessList = new ArrayList<RandomAccessFile>(goodFiles.size());
        for (String orderFile: orderFiles){
            File file = new File(orderFile);
            _orderFiles.add(file);
            _orderAccessList.add(new RandomAccessFile(file,"r"));
        }
        for (String buyerFile: buyerFiles){
            File file = new File(buyerFile);
            _buyerFiles.add(file);
            _buyerAccessList.add(new RandomAccessFile(file,"r"));
        }
        for (String goodFile: goodFiles){
            File file = new File(goodFile);
            _goodFiles.add(file);
            _goodAccessList.add(new RandomAccessFile(file,"r"));
        }

        //读取数据建立索引
        //读取order建立索引
        orderIdMap = new HashMap<Long, Map<String, String>>(1024* 512);
        orderByBuyerMap = new HashMap<String, List<Long>>(1024 * 8);
        orderByGoodMap = new HashMap<String, List<Long>>(1024 * 8);
        for (int i = 0; i < _orderFiles.size(); i++) {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(_orderFiles.get(i))));
            String line = null;
            while (true){
                line = br.readLine();
                if(line == null) break;
                Map<String,String> fields = OrderUtil.parse(line);
                Long orderId = Long.valueOf(fields.get(Constant.ORDER_ID));
                String buyerId = fields.get(Constant.BUYER_ID);
                String goodId = fields.get(Constant.GOOD_ID);
                orderIdMap.put(orderId,fields);
                if(orderByBuyerMap.containsKey(buyerId)){
                    orderByBuyerMap.get(buyerId).add(orderId);
                }else {
                    orderByBuyerMap.put(buyerId,new ArrayList<Long>());
                    orderByBuyerMap.get(buyerId).add(orderId);
                }
                if(orderByGoodMap.containsKey(goodId)){
                    orderByGoodMap.get(goodId).add(orderId);
                }else {
                    orderByGoodMap.put(goodId,new ArrayList<Long>());
                    orderByGoodMap.get(goodId).add(orderId);
                }
            }
            br.close();
        }

        //读取buyer列表
        buyerIdMap = new HashMap<String, Map<String, String>>(1024 * 8);
        for (int i = 0; i < _buyerFiles.size(); i++) {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(_buyerFiles.get(i))));
            String line = null;
            while (true){
                line = br.readLine();
                if(line == null) break;
                Map<String,String> fields = OrderUtil.parse(line);
                String buyerId = fields.get(Constant.BUYER_ID);
                buyerIdMap.put(buyerId,fields);
            }
            br.close();
        }
        //读取good列表
        goodIdMap = new HashMap<String, Map<String, String>>(1024 * 8);
        for (int i = 0; i < _goodFiles.size(); i++) {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(_goodFiles.get(i))));
            String line = null;
            while (true){
                line = br.readLine();
                if(line == null) break;
                Map<String,String> fields = OrderUtil.parse(line);
                String goodId = fields.get(Constant.GOOD_ID);
                goodIdMap.put(goodId,fields);
            }
            br.close();
        }
        //TODO storeFolders 用来存储索引
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
        long orderId;
        Map<String, String> fields = new HashMap<String, String>();

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
    public Result queryOrder(long orderId, Collection<String> keys){
        return getByOrderId(orderId,keys);
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
        if(!orderByBuyerMap.containsKey(buyerid)){
            return new ArrayList<Result>().iterator();
        }
        List<Long> orderIds = orderByBuyerMap.get(buyerid);
        List<Result>  results = new ArrayList<Result>();
        for (Long orderId:orderIds){
            Long ctime = Long.valueOf(orderIdMap.get(orderId).get(Constant.CTIME));
            if(ctime >= startTime && ctime < endTime){
                results.add(getByOrderId(orderId,null));
            }
        }
        Collections.sort(results, new Comparator<Result>() {
            @Override
            public int compare(Result o1, Result o2) {
                try {
                    return Long.valueOf(o2.get(Constant.CTIME).valueAsLong()).compareTo(o1.get(Constant.CTIME).valueAsLong());
                }catch (TypeException e){
                    e.printStackTrace();
                }
                return 0;
            }
        });
        return results.iterator();
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
        //TODO 一个good对应多少个Saler
        if(!orderByGoodMap.containsKey(goodid)){
            new ArrayList<Result>().iterator();
        }
        List<Long> orderIds = orderByGoodMap.get(goodid);
        final List<Result>  results = new ArrayList<Result>();
        for (Long orderId:orderIds){
            results.add(getByOrderId(orderId,keys));
        }
        Collections.sort(results, new Comparator<Result>() {
            @Override
            public int compare(Result o1, Result o2) {
                return  Long.valueOf(o1.orderId()).compareTo(o2.orderId());
            }
        });
        return results.iterator();
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
        if(!orderByGoodMap.containsKey(goodid)){
            return null;
        }
        List<Long> orderIds = orderByGoodMap.get(goodid);
        List<Result>  results = new ArrayList<Result>();
        List<String> keys = new ArrayList<String>(4);
        keys.add(key);
        for (Long orderId:orderIds){
            results.add(getByOrderId(orderId,keys));
        }
        //先假设为long
        {
            Long value = 0l;
            boolean allNull = true;
            boolean correct = true;
            for (Result result : results) {
                if (result.get(key).valueAsString() == null) {
                    continue;
                }
                allNull = false;
                try {
                    value += result.get(key).valueAsLong();
                } catch (TypeException e) {
                    correct = false;
                    break;
                }
            }
            if(correct){
                if (allNull) return null;
                return new BaseKeyValue(key, value.toString());
            }

        }
        {
            Double value = 0.0d;
            boolean allNull = true;
            for (Result result : results) {
                if (result.get(key).valueAsString() == null) {
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
            if (allNull) return null;
            return new BaseKeyValue(key, value.toString());
        }
    }


    private Result getByOrderId(long orderId,Collection<String> keys){
        if(!orderIdMap.containsKey(orderId)) return null;

        if(keys != null && keys.size() == 0){
            return new BaseResult(orderId,new HashMap<String, String>(4));
        }
        Map<String,String> fields = new HashMap<String, String>(8);
        Map<String,String> order = orderIdMap.get(orderId);
        Map<String,String> good = goodIdMap.get(order.get(Constant.GOOD_ID));
        Map<String,String> buyer = buyerIdMap.get(order.get(Constant.BUYER_ID));
        if(keys == null){
            fields.putAll(order);
            fields.putAll(good);
            fields.putAll(buyer);
            return new BaseResult(orderId,fields);
        }
        for (String key:keys){
            if(order.containsKey(key)){
                fields.put(key,order.get(key));
            }
            if(good.containsKey(key)){
                fields.put(key, good.get(key));
            }
            if(buyer.containsKey(key)){
                fields.put(key, buyer.get(key));
            }
        }
        return new BaseResult(orderId, fields);
    }
}
