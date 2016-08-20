package com.alibaba.middleware.race.store;

import com.alibaba.middleware.race.util.TypeUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by liuzhendong on 16/7/15.
 * page的管理器
 * 理论上来说,可以设计成跨文件的,但是这里简化成单个文件;
 * 约定1: 单个对象管理的page都在一个文件中
 * 约定2: 第一个page,用来记录文件的page分配状况,如第一个page在哪个位置,总共分配了多少个page,分配的位置点,删除的page等
 * 约定3: 按顺序依次申请page,但在逻辑上应该理解为page链表,而不是page数组
 *
 */
public class PageMgr {

    public final String path;
    public final Page firstPage;

    public long allocatedPos; //下一个page的分配会从这里开始
    public long firstAllocatedPos; //第一个分配出去的page,除掉了firstPage

    public RandomAccessFile readHandler;
    public final long pageSize = StoreConfig.DEFAULT_PAGE_SIZE;
    public final long areaSize = StoreConfig.DEFAULT_AREA_SIZE;
    //RandomAccessFile 的共用会提高效率,这里把文件划分成多个area,每个area共用一个RandomAccessFile对象,其大小是整数个page的大小
    public Map<Integer,RandomAccessFile> areaMap = new HashMap<Integer, RandomAccessFile>();

    public Map<Long,Page> pageMap = new HashMap<Long, Page>();

    private PageMgr(String path,Page firstPage){
        this.path = path;
        this.firstPage = firstPage;
    }

    public static PageMgr load(String oldPath)throws IOException{
        Page firstPage = new Page();
        PageMgr pageMgr = new PageMgr(oldPath,firstPage);
        pageMgr.readHandler = new RandomAccessFile(pageMgr.path, "rw");
        RandomAccessFile randomAccessFile = new RandomAccessFile(pageMgr.path, "rw");
        firstPage.randomFile(randomAccessFile);
        firstPage.startIndex(0);
        //TODO 初始化 allocatedPos
        pageMgr.loadFirstPage();
        return pageMgr;

    }


    public static PageMgr createNew(String newPath,long size)throws IOException{
        Page firstPage = new Page();
        PageMgr pageMgr = new PageMgr(newPath,firstPage);
        pageMgr.readHandler = new RandomAccessFile(pageMgr.path, "rw");
        RandomAccessFile randomAccessFile = new RandomAccessFile(pageMgr.path, "rw");
        randomAccessFile.setLength(size);
        firstPage.randomFile(randomAccessFile).startIndex(0);
        pageMgr.allocatedPos = StoreConfig.DEFAULT_PAGE_SIZE;
        pageMgr.firstAllocatedPos = pageMgr.allocatedPos;
        pageMgr.flushFirstPage();
        return pageMgr;
    }


    public Page getPageByPos(long pos)throws IOException{
        if(pos > allocatedPos) {
            throw new RuntimeException(String.format("pos out of range %d < %d", allocatedPos, pos));
        }
       return new Page().startIndex(pos).randomFile(allocateAccessFile(pos));
    }


    private RandomAccessFile allocateAccessFile(long pos)throws IOException{
        int areaId = (int) (pos/areaSize);
        if(areaMap.get(areaId) != null){
            return areaMap.get(areaId);
        }
        RandomAccessFile tmp = new RandomAccessFile(this.path,"rw");
        areaMap.put(areaId,tmp);
        return tmp;
    }


    public synchronized Page allocateNewPage()throws IOException{
        Page page = new Page().startIndex(allocatedPos).randomFile(allocateAccessFile(allocatedPos));
        pageMap.put(allocatedPos,page);
        allocatedPos += pageSize;
        return page;
    }


    //前面16个字节,分布存储allocatedPos,firstAllocatedPos
    public void flushFirstPage() throws IOException{
        byte[] body = new byte[16];
        System.arraycopy(TypeUtil.longToBytes(allocatedPos),0,body,0,8);
        System.arraycopy(TypeUtil.longToBytes(firstAllocatedPos),0,body,8,8);
        firstPage.flushIntoDisk(body);
    }


    public void loadFirstPage() throws IOException{
        byte[] body = firstPage.readFromDisk();
        allocatedPos = TypeUtil.bytesToLong(Arrays.copyOfRange(body,0,8));
        firstAllocatedPos = TypeUtil.bytesToLong(Arrays.copyOfRange(body,8,16));
    }


}
