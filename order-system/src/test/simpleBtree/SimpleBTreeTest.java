package simpleBtree;

import com.alibaba.middleware.race.store.SimpleBTree;
import com.alibaba.middleware.race.util.PrintUtil;
import com.alibaba.middleware.race.util.RankUtil;
import com.alibaba.middleware.race.util.TypeUtil;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

/**
 * Created by liuzhendong on 16/7/27.
 */
public class SimpleBTreeTest {


    String baseDir = "/Users/liuzhendong/Source/middleware-race/data/index/";
    Random random = new Random();
    @Test
    public void testWrite()throws Exception{
        final SimpleBTree simpleBTree = new SimpleBTree(baseDir+"test.index",8,8,false);
        int num = 1000* 1000;
        long start = System.currentTimeMillis();
        final List<byte[]> keys = new ArrayList<byte[]>(1024);
        long lastNum = 0;
        for (int i = 0; i < num; i++) {
            byte[] node = new  byte[16];
            long randNum = random.nextLong();
            if(randNum < 0) randNum = randNum >>> 1;

            System.arraycopy(TypeUtil.longToBytes(randNum),0,node,0,8);
            String suffix = convert((int) randNum);
            String value = "va" + suffix +"\n";
            System.arraycopy(value.getBytes(),0,node, 8, 8);
            simpleBTree.write((node));
            if(randNum - lastNum > 50 && keys.size() < 1024){
                lastNum = randNum;
                keys.add(TypeUtil.longToBytes(randNum));
            }
        }
        simpleBTree.finishWrite();
        final long end = System.currentTimeMillis();
        simpleBTree.reRank();
        final long end2 = System.currentTimeMillis();
        PrintUtil.print("create num:%d cost:%d rerank:%d ms", num, end - start, end2 - end);

        PrintUtil.print("keyNodes size:%d", simpleBTree.keyNodes.size());


        Runnable task = new Runnable() {
            @Override
            public void run() {
                try {
                    int testNum = keys.size()-1,succNum = 0;
                    for (int i = 0; i < keys.size() -1; i++) {
                        List<byte[]> res = simpleBTree.query(keys.get(i),keys.get(i+1));
                        boolean succ = true;
                        for (int j = 0; j < res.size(); j++) {
                            System.out.println(String.format("key:%d value:%d", TypeUtil.bytesToLong(keys.get(i)),
                                    TypeUtil.bytesToLong(Arrays.copyOfRange(res.get(j), 0, 8))));
                            if(RankUtil.compare(res.get(j),keys.get(i), 8) >= 0
                                    && RankUtil.compare(res.get(j), keys.get(i+1),8) < 0 ){

                            }else {
                                succ = false;
                            }
                            if(j > 0){
                                if(RankUtil.compare(res.get(j),res.get(j-1), 8) >= 0){

                                }else {
                                    succ  = false;
                                }
                            }
                            //PrintUtil.print("from:%s to:%s value:%s",new String(keys.get(i)), new String(keys.get(i+1)), new String(res.get(j)));
                        }
                        if (succ) succNum++;
                    }
                    long end3 = System.currentTimeMillis();
                    PrintUtil.print("testNum:%d succNum:%d cost:%d ms",testNum, succNum, end3-end2);
                }catch (Exception e){
                    e.printStackTrace();
                }

            }
        };
        //test
        int tsNum = 5;
        Thread[] ts = new Thread[tsNum];
        for (int i = 0; i < tsNum; i++) {
            ts[i] = new Thread(task);
        }
        for (int i = 0; i < tsNum; i++) {
            ts[i].start();
        }
        for (int i = 0; i < tsNum; i++) {
            ts[i].join();
        }

    }


    private String convert(int num){
        String tmp = Integer.toHexString(num);
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
