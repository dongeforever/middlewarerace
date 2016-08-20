package btree;

import com.alibaba.middleware.race.store.BTree;
import com.alibaba.middleware.race.util.PrintUtil;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

/**
 * Created by liuzhendong on 16/7/19.
 */
public class BTreeInDiskTest {

    private Random random = new Random();



    @Test
    public void testReadFromDisk()throws IOException{
        BTree bTree = BTree.loadBTreeFromDisk("/Users/liuzhendong/Source/middleware-race/data/index/test.index");
        bTree.debug = false;
        String key = "key04735";
        List<byte[]> res= bTree.query(key.getBytes());
        for (int j = 0; j < res.size(); j++) {
            System.out.println(new String(res.get(j)));
        }
    }

    public void testFlushToDisk(String file)throws IOException{
        BTree bTree = BTree.createBTreeWithDisk(file);
        bTree.debug = false;
        int num = 10*1000*1000;
        List<String> keys = new ArrayList<String>();
        long start = System.currentTimeMillis();
        for (int i = 0; i < num; i++) {
            String suffix = convert(i);
            String key = "key"+suffix;
            String value = "value"+suffix;
            //System.out.println(String.format("key:%s value:%s",key,value));
            bTree.insert(key.getBytes(), value.getBytes());
            if(i % 100 == 0){
                System.out.println(i);
            }
        }
        long end = System.currentTimeMillis();
        bTree.flushBTree();
        long end2 = System.currentTimeMillis();
        PrintUtil.print("create:%d ms, flush:%d ms",end - start, end2 - end);

    }


    @Test
    public void testMultiFlushToDisk()throws Exception{
         class  Task implements Runnable{

            public String file;
            public Runnable file(String file){
                this.file = file;
                return this;
            }
            @Override
            public void run() {
                try {
                    testFlushToDisk(this.file);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        };
        int tsNum = 1;
        Thread[] ts = new Thread[tsNum];

        for (int i = 0; i < tsNum; i++) {
            ts[i] = new Thread(new Task().file("/Users/liuzhendong/Source/middleware-race/data/index/test.index." + i));
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < tsNum; i++) {
            ts[i].start();
        }
        for (int i = 0; i < tsNum; i++) {
            ts[i].join();
        }
        long end = System.currentTimeMillis();
        PrintUtil.print("cost:%d ms",end - start);

    }

    private String convert(int num){
        String tmp = num+"";
        if(tmp.length() >= 5) return tmp.substring(0,5);
        switch (tmp.length()){
            case 1:
                return "0000" + tmp;
            case 2:
                return "000" + tmp;
            case 3:
                return "00" + tmp;
            case 4:
                return "0" + tmp;
        }
        return "00000";
    }
}
