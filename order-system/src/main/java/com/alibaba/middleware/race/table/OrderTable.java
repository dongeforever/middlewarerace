package com.alibaba.middleware.race.table;

import com.alibaba.middleware.race.model.Constant;
import com.alibaba.middleware.race.model.GoodOrderKey;
import com.alibaba.middleware.race.model.OrderKey;
import com.alibaba.middleware.race.store.*;
import com.alibaba.middleware.race.util.OrderUtil;
import com.alibaba.middleware.race.util.PrintUtil;
import com.alibaba.middleware.race.util.RankUtil;
import com.alibaba.middleware.race.util.TypeUtil;
import com.sun.glass.ui.SystemClipboard;
import com.sun.org.apache.xpath.internal.operations.Or;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

/**
 * Created by liuzhendong on 16/7/22.
 * //订单表的设计
 * 约定1:固定有的字段存储在B+树中,不确定的字段存储在原文件
 * 约定2:缓存淘汰策略,LRU
 * 约定3:goodid和buyerid先hash再存储,注意过滤碰撞问题
 * 约定4:注意分表,分开存储索引
 *
 * 先大刀阔斧地写起来,然后再优化
 */
public class OrderTable {
    final List<RandomAccessFile> orderAccesses; //原始文件存储
    final List<String> storeFolders; //存储索引

    public final byte orderIdKeyLen = 8;
    public final byte buyerKeyLen = 16;
    public final byte goodKeyLen = 16; //12 变成原来的4倍

    //分表的索引
    public List<SimpleBTree>  bTreesById;
    public List<SimpleBTree>  bTreesByBuyer;
    public List<SimpleBTree>  bTreesByGood;


    //基于buyer id切分存储,把同一个buyer的order切到同一个文件中
    public List<OriginStore>  originStoresByBuyer;

    //TODO 订单缓存
    //LruCache<String,Map<String,String>> orderCacheByBuyerPos;
    //LruCache<String, List<Map<String,String>>> orderCacheByGood;

    public OrderTable(Collection<String> orderFiles, Collection<String> originStoreFolders)throws IOException{
        //TODO 如果超出预期 则需要修改程序
        if(orderFiles.size() > Short.MAX_VALUE) throw new RuntimeException("订单文件个数超出预期"+orderFiles.size());
        orderAccesses = new ArrayList<RandomAccessFile>(orderFiles.size());
        for (String orderFile: orderFiles){
            orderAccesses.add(new RandomAccessFile(orderFile,"r"));
        }
        storeFolders = new ArrayList<String>(originStoreFolders);
        int size = storeFolders.size();
        bTreesById = new ArrayList<SimpleBTree>(StoreConfig.ORDER_PARTITION_NUM);
        bTreesByBuyer = new ArrayList<SimpleBTree>(StoreConfig.ORDER_PARTITION_NUM);
        bTreesByGood = new ArrayList<SimpleBTree>(StoreConfig.ORDER_PARTITION_NUM);

        originStoresByBuyer = new ArrayList<OriginStore>(StoreConfig.ORDER_ORIGIN_STORE_NUM);

        //btree 初始化
        for (int i = 0; i < StoreConfig.ORDER_PARTITION_NUM; i++) {
            int index = i % size;
            bTreesById.add(new SimpleBTree(storeFolders.get(index)+"/index_orderid." + i,orderIdKeyLen,58,true).name("bTreesById-" + i));
        }
        for (int i = 0; i < StoreConfig.ORDER_PARTITION_NUM; i++) {
            int index = i % size;
            bTreesByBuyer.add(new SimpleBTree(storeFolders.get(index)+"/index_buyer_ctime." + i,buyerKeyLen,8,false));
        }
        for (int i = 0; i < StoreConfig.ORDER_PARTITION_NUM; i++) {
            int index = i % size;
            bTreesByGood.add(new SimpleBTree(storeFolders.get(index)+"/index_good_orderid." + i, goodKeyLen,37,true).name("bTreesByGood" + i));
        }

        for (int i = 0; i < StoreConfig.ORDER_ORIGIN_STORE_NUM; i++) {
            int index = i % size;
            originStoresByBuyer.add(new OriginStore(storeFolders.get(index)+"/origin_buyer_ctime." + i));
        }

    }
    public class BuildTask implements Runnable {

        public int id;
        public BuildTask id(int id){
            this.id = id;
            return this;
        }
        @Override
        public void run() {
            try {
                for(int i = 0; i < orderAccesses.size(); i++) {
                    if(i % StoreConfig.ORDER_BUILD_THREAD_NUM != this.id){
                        continue;
                    }
                    RandomAccessFile tmpAccess = orderAccesses.get(i);
                    PrintUtil.print("order id:%d file:%s",this.id, tmpAccess.toString());
                    long currPos = 0;
                    int offset = 0;
                    byte[]  buff = new byte[32 * 1024];
                    byte[] fileId = TypeUtil.shortToBytes((short)i);
                    while (true){
                        int tmpLen = buff.length-offset;
                        int readNum = tmpAccess.read(buff,offset, tmpLen);
                        if (readNum ==  0){
                            break;
                        }
                        int last = 0;
                        int j = 0;
                        for (; j < readNum + offset; j++) {
                            if(buff[j] == '\n'){
                                String str = new String(buff,last,j-last);
                                parseLine(fileId, currPos+last,str);
                                last = j + 1;
                            }
                        }
                        //TODO 这个地方有坑,如果last为0呢
                        if(last == 0) throw new RuntimeException("parse line error,last is 0");
                        currPos = currPos + readNum + offset;
                        if(last < readNum + offset){
                            offset = readNum + offset - last;
                            System.arraycopy(buff,last,buff,0,offset);
                        }else {
                            offset = 0;
                        }
                        //往前退一点
                        currPos = currPos - offset;
                        if(readNum != tmpLen) break;
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }

        }
    };
    public void buildIndex()throws IOException,InterruptedException{
        int tsNum = StoreConfig.ORDER_BUILD_THREAD_NUM;
        Thread[] ts = new Thread[tsNum];
        for (int i = 0; i < tsNum; i++) {
            ts[i] = new Thread(new BuildTask().id(i));
        }
        for (int i = 0; i < tsNum; i++) {
            ts[i].start();
        }
        for (int i = 0; i < tsNum; i++) {
            ts[i].join();
        }
        for (SimpleBTree stree : bTreesById){
            stree.finishWrite();
        }
        for (SimpleBTree stree : bTreesByBuyer){
            stree.finishWrite();
        }
        for (SimpleBTree stree : bTreesByGood){
            stree.finishWrite();
        }
        for (OriginStore originStore : originStoresByBuyer){
            originStore.finishWrite();
        }
        FlushHelper.flush(bTreesById);
        FlushHelper.flush(bTreesByGood);
        FlushHelper.flush(bTreesByBuyer);
        //Thread.sleep(3000);
        //this.orderCacheByBuyerPos = new LruCache<String, Map<String, String>>(500 * 1000, 1000*1000, "orderCacheByBuyerPos");
        //this.orderCacheByGood = new LruCache<String, List<Map<String, String>>>(10* 1000, 10 * 1000, "orderCacheByGood");

    }

    private void parseLine(byte[] fileId,long pos,String line)throws IOException{
        //System.out.println("parse_line:" + line);
        byte[] posBytes = TypeUtil.longToBytes(pos);
        posBytes[0] = fileId[0];posBytes[1] = fileId[1];
        Map<String,String> fields = OrderUtil.parse(line);

        Long orderId = Long.valueOf(fields.get(Constant.ORDER_ID));
        String goodId = fields.get(Constant.GOOD_ID);
        String buyerId = fields.get(Constant.BUYER_ID);
        boolean done = Boolean.valueOf(fields.get(Constant.DONE));
        long ctime = Long.valueOf(fields.get(Constant.CTIME));
        byte amount = fields.get(Constant.AMOUNT) == null ? 0 : Byte.valueOf(fields.get(Constant.AMOUNT));
        byte[] orderIdBytes = TypeUtil.longToBytes(orderId);
        int buyerId_h = OrderUtil.hash(buyerId);
        int goodId_h = OrderUtil.hash(goodId);
        byte[] idNode = new byte[66];
        //order key
        System.arraycopy(orderIdBytes,0,idNode,0,8);
        System.arraycopy(TypeUtil.longToBytes(ctime),0,idNode,8,8);
        idNode[16] = done ? (byte) (amount | 0x80) : amount;
        System.arraycopy(buyerId.getBytes(),0,idNode,17,20);
        System.arraycopy(OrderUtil.makeGoodId(goodId).getBytes(), 0, idNode, 37, 21);
        System.arraycopy(posBytes,0,idNode,58,8);
        //buyer
        byte[] buyerNode = new byte[24];
        short originShard = shardOriginByBuyer(buyerId_h);
        long originPos = originStoresByBuyer.get(originShard).write(line);
        byte[] buyerPosBytes = TypeUtil.longToBytes(originPos);
        System.arraycopy(TypeUtil.shortToBytes(originShard),0,buyerPosBytes,0,2);

        System.arraycopy(buyerId.substring(buyerId.length()-4, buyerId.length()).getBytes(), 0, buyerNode, 0, 4);
        System.arraycopy(TypeUtil.intToBytes(buyerId_h),0,buyerNode,4,4);
        System.arraycopy(TypeUtil.longToBytes(ctime),0,buyerNode,8,8);
        System.arraycopy(buyerPosBytes,0,buyerNode,16,8);

        //good Order
        byte[] goodBytes = new byte[53];
        System.arraycopy(goodId.substring(goodId.length() - 4, goodId.length()).getBytes(),0,goodBytes,0,4);
        System.arraycopy(TypeUtil.intToBytes(goodId_h),0, goodBytes,4,4);
        System.arraycopy(TypeUtil.longToBytes(orderId), 0 , goodBytes, 8, 8);
        System.arraycopy(TypeUtil.longToBytes(ctime), 0, goodBytes, 16 , 8);
        goodBytes[24] = done ? (byte) (amount | 0x80) : amount;
        System.arraycopy(buyerId.getBytes(),0, goodBytes, 25, 20);
        System.arraycopy(posBytes, 0, goodBytes,45, 8);


        int orderIdShard = shardByOrderId(orderId);
        int buyerShard = shardByBuyer(buyerId_h);
        int goodShard = shardByGood(goodId_h);

        bTreesById.get(orderIdShard).write(idNode);
        bTreesByBuyer.get(buyerShard).write(buyerNode);
        bTreesByGood.get(goodShard).write(goodBytes);
    }

    public  long rankCost = 0;
    public List<Map<String,String>> getOrdersByBuyer(String buyerid, long startTime, long endTime)throws IOException{
        int h = OrderUtil.hash(buyerid);
        byte[] from = new byte[16];
        byte[] to = new byte[16];
        System.arraycopy(buyerid.substring(buyerid.length()-4, buyerid.length()).getBytes(),0, from, 0,4);
        System.arraycopy(TypeUtil.intToBytes(h),0,from,4,4);
        System.arraycopy(TypeUtil.longToBytes(startTime),0,from,8,8);

        System.arraycopy(buyerid.substring(buyerid.length()-4, buyerid.length()).getBytes(),0, to, 0,4);
        System.arraycopy(TypeUtil.intToBytes(h),0,to,4,4);
        System.arraycopy(TypeUtil.longToBytes(endTime),0,to,8,8);
        List<byte[]> values = bTreesByBuyer.get(shardByBuyer(h)).query(from,to);
        if(values.size() == 0){
            return new ArrayList<Map<String, String>>(4);
        }

        long[] posArray = new long[values.size()];
        short fileId = (short) TypeUtil.bytesToLong(Arrays.copyOfRange(values.get(0),16,18));
        for (int i = 0; i < values.size(); i++) {
            long pos = TypeUtil.bytesToLong(Arrays.copyOfRange(values.get(i),18,24));
            posArray[i] = pos;
        }
        List<Map<String,String>> result = originStoresByBuyer.get(fileId).getObjectsByPosArray(posArray);
        /*
        {
            //性能测试
            long start = System.currentTimeMillis();
            int num = 10000;
            for (int i = 0; i < num; i++) {
                List<Map<String,String>> tmp = originStoresByBuyer.get(fileId).getObjectsByPosArray(posArray);
            }
            long end = System.currentTimeMillis();
            PrintUtil.print("num:%d cost:%d size:%d",num, end - start, result.size());

        }
        */
        long start = System.currentTimeMillis();
        Collections.sort(result, new Comparator<Map<String, String>>() {
            @Override
            public int compare(Map<String, String> o1, Map<String, String> o2) {
                return Long.valueOf(o1.get(Constant.CTIME)).compareTo(Long.valueOf(o2.get(Constant.CTIME)));
            }
        });
        long end = System.currentTimeMillis();
        rankCost += end-start;
        return result;
    }


    public List<GoodOrderKey> getGoodKeysByGood(String goodId)throws Exception{
        int good_h = OrderUtil.hash(goodId);
        byte[] from = new byte[16];
        byte[] to = new byte[16];
        System.arraycopy(goodId.substring(goodId.length()-4,goodId.length()).getBytes(),0,from,0,4);
        System.arraycopy(TypeUtil.intToBytes(good_h),0,from,4,4);
        System.arraycopy(TypeUtil.longToBytes(0),0,from,8,8);

        System.arraycopy(goodId.substring(goodId.length()-4,goodId.length()).getBytes(),0,to,0,4);
        System.arraycopy(TypeUtil.intToBytes(good_h),0,to,4,4);
        System.arraycopy(TypeUtil.longToBytes(Long.MAX_VALUE),0,to,8,8);

        List<byte[]> values = bTreesByGood.get(shardByGood(good_h)).query(from,to);
        //PrintUtil.print("getGoodKeysByGoodInner %s %d", good_h, values.size());
        if(values.size() == 0){
            return new ArrayList<GoodOrderKey>(4);
        }
        List<GoodOrderKey> goodOrderKeys = new ArrayList<GoodOrderKey>(values.size());
        for (byte[] bytes : values){
            goodOrderKeys.add(GoodOrderKey.parse(bytes));
        }
        return goodOrderKeys;
    }

    public List<Map<String, String>> getOrderByKeys(List<GoodOrderKey> keys)throws Exception{
        List<Map<String,String>> result = new ArrayList<Map<String, String>>();
        for (GoodOrderKey key : keys){
            short fildId = (short) TypeUtil.bytesToLong(Arrays.copyOfRange(key.pos,0,2));
            long pos = TypeUtil.bytesToLong(Arrays.copyOfRange(key.pos,2,8));
            Map<String,String>  tmpRes = getOrderByPos(fildId, pos,false);
            result.add(tmpRes);
        }
        return result;
    }
    public List<Map<String, String>> getOrdersByGood(String goodId)throws Exception{
        return getOrderByKeys(getGoodKeysByGood(goodId));
    }

    public OrderKey  getOrderKey(long orderId)throws Exception{
        List<byte[]> values = bTreesById.get(shardByOrderId(orderId)).query(TypeUtil.longToBytes(orderId));
        if(values.size() == 0){
            return null;
        }
        return OrderKey.parse(values.get(0));
    }
    public Map<String,String> getOrderByKey(OrderKey orderKey)throws Exception{
        if(orderKey == null) return null;
        short fildId = (short) TypeUtil.bytesToLong(Arrays.copyOfRange(orderKey.pos,0,2));
        long pos = TypeUtil.bytesToLong(Arrays.copyOfRange(orderKey.pos,2,8));
        return getOrderByPos(fildId,pos,false);
    }
    public Map<String,String> getOrderById(long orderId)throws Exception{
        return getOrderByKey(getOrderKey(orderId));
    }
    private Map<String,String> getOrderByPos(short fileId,long pos, boolean cache)throws IOException{
        /*
        if(cache){
            Map<String,String> cachedOrder = orderCacheByBuyerPos.get(fileId+""+pos);
            if(cachedOrder != null) return cachedOrder;
        }
        */
        String line = "";
        synchronized (orderAccesses.get(fileId)){
            line = OrderUtil.readLine(orderAccesses.get(fileId), pos,250);
        }
        //System.out.println(line);
        Map<String,String> order = OrderUtil.parse(line);
        //if(cache) orderCacheByBuyerPos.put(fileId+""+pos, order);
        return order;
    }

    public byte shardByOrderId(long orderId){
        return (byte) (orderId % bTreesById.size());
    }
    public byte shardByBuyer(int buyerId_h){
        return (byte) (buyerId_h % bTreesByBuyer.size());
    }
    public byte shardByGood(int goodId_h){
        return (byte) (goodId_h % bTreesByGood.size());
    }


    public short shardOriginByBuyer(int buyerId_h){
        return (short) (buyerId_h % originStoresByBuyer.size());
    }


}
