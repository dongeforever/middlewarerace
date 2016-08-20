package com.alibaba.middleware.race.table;

import com.alibaba.middleware.race.model.Constant;
import com.alibaba.middleware.race.store.FlushHelper;
import com.alibaba.middleware.race.store.LruCache;
import com.alibaba.middleware.race.store.SimpleBTree;
import com.alibaba.middleware.race.util.OrderUtil;
import com.alibaba.middleware.race.util.PrintUtil;
import com.alibaba.middleware.race.util.TypeUtil;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

/**
 * Created by liuzhendong on 16/7/22.
 * 类似于order table
 */
public class GoodTable {
    final List<RandomAccessFile> originAccesses; //原始文件存储
    final List<String> storeFolders; //存储索引

    public final int indexKeyLen = 8;

    public final int partitionNum = 4; //4

    public final int buildThreadNum = 4; //4

    //分表的索引
    public List<SimpleBTree>  bTreesById;

    //LruCache<String,Map<String,String>>  goodCache;

    public GoodTable(Collection<String> goodFiles, Collection<String> originStoreFolders)throws IOException{
        //TODO 如果超出预期 则需要修改程序
        if(goodFiles.size() > Short.MAX_VALUE) throw new RuntimeException("订单文件个数超出预期"+ goodFiles.size());
        originAccesses = new ArrayList<RandomAccessFile>(goodFiles.size());
        for (String orderFile: goodFiles){
            originAccesses.add(new RandomAccessFile(orderFile,"r"));
        }
        storeFolders = new ArrayList<String>(originStoreFolders);
        int size = storeFolders.size();
        bTreesById = new ArrayList<SimpleBTree>(partitionNum);
        //btree 初始化
        for (int i = 0; i < partitionNum; i++) {
            int index = i % size;
            bTreesById.add(new SimpleBTree(storeFolders.get(index)+"/index_goodid." + i,indexKeyLen,8,false));
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
                for(int i = 0; i < originAccesses.size(); i++) {
                    if(i % buildThreadNum != this.id){
                        continue;
                    }
                    RandomAccessFile tmpAccess = originAccesses.get(i);
                    PrintUtil.print("good id:%d file:%s",this.id, tmpAccess.toString());
                    long currPos = 0;
                    int offset = 0;
                    byte[]  buff = new byte[128 * 1024];
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
                        currPos = currPos + readNum + offset;
                        if(last == 0) throw new RuntimeException("parse line error,last is 0");
                        if(last < readNum + offset){
                            offset = readNum + offset - last;
                            System.arraycopy(buff,last,buff,0,offset);
                        }else {
                            offset = 0;
                        }
                        //往前退一点
                        currPos = currPos - offset;
                        if(readNum != tmpLen) {
                            PrintUtil.print("readNum:%d tmpLen:%d",readNum,tmpLen);
                            break;
                        }

                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }

        }
    };
    public void buildIndex()throws IOException,InterruptedException{
        int tsNum = buildThreadNum;
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
        FlushHelper.flush(bTreesById);
        //Thread.sleep(3000);
        //this.goodCache = new LruCache<String, Map<String, String>>(200 * 1000, 400*1000,"goodCache");

    }

    private void parseLine(byte[] fileId,long pos,String line)throws IOException{
        //System.out.println("parse_line:" + line);
        byte[] posBytes = TypeUtil.longToBytes(pos);
        posBytes[0] = fileId[0];posBytes[1] = fileId[1];
        Map<String,String> fields = OrderUtil.parse(line);
        String goodId = fields.get(Constant.GOOD_ID);

        int goodId_h = OrderUtil.hash(goodId);
        byte[] goodNode = new byte[16];

        System.arraycopy(goodId.substring(goodId.length()-4,goodId.length()).getBytes(),0, goodNode, 0 , 4);
        System.arraycopy(TypeUtil.intToBytes(goodId_h),0,goodNode,4,4);
        System.arraycopy(posBytes,0,goodNode,8,8);

        int goodShard = shardByGood(goodId_h);

        bTreesById.get(goodShard).write(goodNode);
    }


    public Map<String, String> getGoodById(String goodId)throws Exception{
        //Map<String,String> cachedGood = goodCache.get(goodId);
        //if(cachedGood != null) return cachedGood;

        int good_h = OrderUtil.hash(goodId);
        byte[] key  = new  byte[8];
        System.arraycopy(goodId.substring(goodId.length()-4, goodId.length()).getBytes(),0,key,0,4);
        System.arraycopy(TypeUtil.intToBytes(good_h),0, key, 4, 4);
        List<byte[]> values = bTreesById.get(shardByGood(good_h)).query(key);
        if(values.size() == 0){
            return new HashMap<String, String>(4);
        }
        for (byte[] value : values){
            short fildId = (short) TypeUtil.bytesToLong(Arrays.copyOfRange(value,8,10));
            long pos = TypeUtil.bytesToLong(Arrays.copyOfRange(value,10,16));
            Map<String,String>  tmpRes = getGoodByPos(fildId, pos);
            if(tmpRes.get(Constant.GOOD_ID).equals(goodId)){
                //goodCache.put(goodId, tmpRes);
                return tmpRes;
            }
        }
        return new HashMap<String, String>(4);
    }
    private Map<String,String> getGoodByPos(short fileId,long pos)throws IOException{
        String line = "";
        synchronized (originAccesses.get(fileId)){
            line = OrderUtil.readLine(originAccesses.get(fileId), pos, 1024);
        }
        //System.out.println(line);
        return OrderUtil.parse(line);
    }

    public byte shardByGood(int goodId_h){
        return (byte) (goodId_h % bTreesById.size());
    }



}
