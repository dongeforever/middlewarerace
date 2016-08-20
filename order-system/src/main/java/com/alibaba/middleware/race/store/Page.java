package com.alibaba.middleware.race.store;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created by liuzhendong on 16/7/15.
 *
 * 内存和磁盘交换的基本单位
 * 约定1: 所有内容采取覆盖制,前面4个字节,表示该page写入的内容的长度
 * 约定2: 调用方自己控制内容的长度,如果这里发现内容超过page的范围,则自动截断,读和写均截断
 */
public class Page {


    private  RandomAccessFile  randomFile; //原始的随机读写文件句柄  //共用一个句柄,可以提高效率
    private  long  startIndex;   //在原文件中的初始位置
    private  final int  size = StoreConfig.DEFAULT_PAGE_SIZE; // 默认大小是16K
    private  final int  headSize = 16; //page header,前面四个字节,存储该page写入的内容大小,注意,不包括头部


    public Page randomFile(RandomAccessFile randomFile){
        this.randomFile = randomFile;
        return this;
    }
    public Page startIndex(long startIndex){
        this.startIndex = startIndex;
        return this;
    }

    //从磁盘中读出内容
    public byte[] readFromDisk()throws IOException{
        if(randomFile == null) {
            throw new RuntimeException("random file object is not allocated");
        }
        synchronized (randomFile){
            randomFile.seek(startIndex);
            int bodySize = randomFile.readInt();
            if(bodySize > size - headSize) bodySize = size - headSize;
            byte[] body = new byte[bodySize];
            randomFile.seek(startIndex + headSize);
            randomFile.read(body);
            return body;
        }
    };

    //刷入内容,注意要扣除头部
    public void flushIntoDisk(byte[] body)throws IOException{
        if(randomFile == null){
            throw  new RuntimeException("random file object is not allocated");
        }
        synchronized (randomFile){
            randomFile.seek(startIndex);
            int bodysize = body.length > size - headSize ? size - headSize : body.length;
            randomFile.writeInt(bodysize);
            randomFile.seek(startIndex + headSize);
            randomFile.write(body,0, bodysize);
            randomFile.getFD().sync();
        }
    }



    public int getPageSize(){
        return this.size;
    }
    public long getStartIndex(){
        return this.startIndex;
    }

}
