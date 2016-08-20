package com.alibaba.middleware.race.store;

import com.alibaba.middleware.race.util.OrderUtil;
import com.alibaba.middleware.race.util.PrintUtil;
import org.omg.PortableServer.LIFESPAN_POLICY_ID;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by liuzhendong on 16/8/2.
 * 存储原始内容,以换行符分割
 */
public class OriginStore {

    ByteBuffer byteBuffer;
    FileChannel fileChannel;
    AtomicLong currWritePos;
    String path;

    public OriginStore(String filePath)throws IOException{
        byteBuffer = ByteBuffer.allocateDirect(StoreConfig.DEFAULT_ORIGIN_BUFF_LEN);
        this.path = filePath;
        fileChannel = new RandomAccessFile(filePath,"rw").getChannel();
        currWritePos = new AtomicLong(0);
    }

    public synchronized long write(String line)throws IOException{
        byte[] bytes = (line+"\n").getBytes();
        if(byteBuffer.remaining() < bytes.length){
            byteBuffer.flip();
            fileChannel.write(byteBuffer);
            byteBuffer.clear();
        }
        byteBuffer.put(bytes);
        return currWritePos.getAndAdd(bytes.length);
    }

    public void finishWrite()throws IOException{
        byteBuffer.flip();
        fileChannel.write(byteBuffer);
        byteBuffer.clear();
        byteBuffer = null;//gc
    }


    public List<Map<String,String>> getObjectsByPosArray(long[] posArray)throws IOException{
        Arrays.sort(posArray);
        //这里固定一次只读2M,可以考虑切分得更多
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024 * 1024);
        List<Map<String,String>> result = new ArrayList<Map<String, String>>(posArray.length);
        long start = posArray[0];
        int index = 0;
        //循环取数据
        //System.out.println("getObjectsByPosArray:" + posArray.length);
        do{
            int readNum = 0;
            synchronized (fileChannel){
                byteBuffer.clear();
                fileChannel.position(start);
                readNum = fileChannel.read(byteBuffer);
                byteBuffer.flip();
            }
            if(readNum == 0) break;
            for (;index < posArray.length; index++){
                if(posArray[index] + 100 < start + readNum){
                    byteBuffer.position((int) (posArray[index] - start));
                    if(parseAndAdd(byteBuffer, result)){
                        //go
                        //System.out.println(index);
                    }else {
                        //实际上不足了
                        if(start == posArray[index]) PrintUtil.print("parse死循环了 %d %d",start, readNum);

                        start = posArray[index];
                        break;
                    }
                }else {
                    if(start == posArray[index]) PrintUtil.print("死循环了 %d %d",start, readNum);
                    start = posArray[index];
                    break;
                }
            }
        }while (index < posArray.length);

        if(result.size() != posArray.length){
            PrintUtil.print("UNEXPECT %d != %d", result.size(), posArray.length);
            System.exit(1);
        }
        return result;
    }


    public boolean parseAndAdd(ByteBuffer buffer,List<Map<String,String>> result){
        byte[] bytes = new  byte[32 * 1024];
        int count = 0;
        boolean corr = false;
        int remain = buffer.remaining();
        for (int i=0;i< remain;i++){
            byte tmp = buffer.get();
            if(tmp == (int)'\n'){
                corr = true;
                break;
            }
            bytes[count++] = tmp;
            if(count == bytes.length){
                System.out.println(new String(bytes));
                break;
            }
        }
        if(corr){
            String line = new String(bytes,0,count);
            result.add(OrderUtil.parse(line));
        }
        return corr;
    }

}
