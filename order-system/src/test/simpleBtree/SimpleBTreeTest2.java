package simpleBtree;

import com.alibaba.middleware.race.store.SimpleBTree;
import com.alibaba.middleware.race.util.PrintUtil;
import com.alibaba.middleware.race.util.RankUtil;
import com.alibaba.middleware.race.util.TypeUtil;
import org.junit.Test;

import java.util.*;

/**
 * Created by liuzhendong on 16/7/27.
 */
public class SimpleBTreeTest2 {


    String baseDir = "/Users/liuzhendong/Source/middleware-race/data/index/";
    Random random = new Random();
    @Test
    public void testSingleQueryByStr()throws Exception{
        final SimpleBTree simpleBTree = new SimpleBTree(baseDir+"test.index",8,8,false);
        int num = 1000* 1000;
        long start = System.currentTimeMillis();
        long lastNum = 0;
        Map<Integer,Integer> keys = new HashMap<Integer, Integer>(2014);
        for (int i = 0; i < num; i++) {
            int randNum = random.nextInt(num* 100);
            String suffix = convert(randNum);
            String key = "key" + suffix;
            String value = "va" + suffix +"\n";
            simpleBTree.write((key+value).getBytes());

            if(randNum >= 100 && randNum < 10000){
                if(keys.containsKey(randNum)){
                    keys.put(randNum, keys.get(randNum)+1);
                }else {
                    keys.put(randNum,1);
                }
            }
        }
        simpleBTree.finishWrite();
        final long end = System.currentTimeMillis();
        simpleBTree.reRank();
        final long end2 = System.currentTimeMillis();
        PrintUtil.print("create num:%d cost:%d rerank:%d ms", num, end - start, end2 - end);

        PrintUtil.print("kvNum:%d",simpleBTree.kvNum);

        int testNum = keys.size(),succNum = 0;
        for (Integer key : keys.keySet()){
            List<byte[]> res = simpleBTree.query(("key" + convert(key)).getBytes());
            boolean succ = true;
            if(res.size() != keys.get(key)){
                PrintUtil.print("shouldNum:%d realNum:%d key:%d", keys.get(key),res.size(), key);
                succ = false;
            }
            for (byte[] tmp :res){
                String kv = new String(tmp);
                String kv2 = "key"+convert(key)+"va" + convert(key) +"\n";
                if(!kv.equals(kv2)) {
                    succ = false;
                    PrintUtil.print("%s != %s", kv, kv2);

                }

            }
            if(succ) succNum++;
        }
        System.out.println(String.format("testNum:%d succNum:%d",testNum, succNum));
    }

    @Test
    public void testSingleQuery()throws Exception{
        final SimpleBTree simpleBTree = new SimpleBTree(baseDir+"test.index",4,8,false);
        int num = 10* 1000;
        long start = System.currentTimeMillis();
        long lastNum = 0;
        Map<Integer,Integer> keys = new HashMap<Integer, Integer>(2014);
        for (int i = 0; i < num; i++) {
            byte[] node = new  byte[12];
            int randNum = random.nextInt(num* 100);
            while (randNum == 0) randNum = random.nextInt(num*100);
            System.arraycopy(TypeUtil.intToBytes(randNum),0,node,0,4);
            String suffix = convert(randNum);
            String value = "va" + suffix +"\n";
            System.arraycopy(value.getBytes(),0,node, 4, 8);
            simpleBTree.write((node));
            if(randNum >= 100 && randNum < 10000){
                if(keys.containsKey(randNum)){
                    keys.put(randNum, keys.get(randNum)+1);
                }else {
                    keys.put(randNum,1);
                }
            }
        }
        simpleBTree.finishWrite();
        final long end = System.currentTimeMillis();
        simpleBTree.reRank();
        final long end2 = System.currentTimeMillis();
        PrintUtil.print("create num:%d cost:%d rerank:%d ms", num, end - start, end2 - end);

        PrintUtil.print("kvNum:%d",simpleBTree.kvNum);

        int testNum = keys.size(),succNum = 0;
        for (Integer key : keys.keySet()){
            List<byte[]> res = simpleBTree.query(TypeUtil.intToBytes(key));
            boolean succ = true;
            if(res.size() != keys.get(key)){
                PrintUtil.print("shouldNum:%d realNum:%d key:%d", keys.get(key),res.size(), key);
                succ = false;
            }
            for (byte[] tmp :res){
                int a = (int) TypeUtil.bytesToLong(Arrays.copyOfRange(tmp,0,4));
                if(a != key) {
                    succ = false;
                    PrintUtil.print("%d != %d", a, key);

                }

            }
            if(succ) succNum++;
        }
        System.out.println(String.format("testNum:%d succNum:%d",testNum, succNum));
    }



    @Test
    public void testRangeQuery()throws Exception{
        final SimpleBTree simpleBTree = new SimpleBTree(baseDir+"test.index",4,8,false);
        int num = 100* 1000;
        long start = System.currentTimeMillis();
        List<Integer> keys = new ArrayList<Integer>(1024);

        int keyStart = 100,keyEnd = 10000;
        for (int i = 0; i < num; i++) {
            byte[] node = new  byte[12];
            int randNum = random.nextInt(num* 100);
            while (randNum == 0) randNum = random.nextInt(num*100);
            System.arraycopy(TypeUtil.intToBytes(randNum),0,node,0,4);
            String suffix = convert(randNum);
            String value = "va" + suffix +"\n";
            System.arraycopy(value.getBytes(),0,node, 4, 8);
            simpleBTree.write((node));
            if(randNum >= keyStart && randNum < keyEnd){
                keys.add(randNum);
            }
        }
        simpleBTree.finishWrite();
        final long end = System.currentTimeMillis();
        simpleBTree.reRank();
        final long end2 = System.currentTimeMillis();
        PrintUtil.print("create num:%d cost:%d rerank:%d ms", num, end - start, end2 - end);

        PrintUtil.print("keyNodes size:%d", simpleBTree.keyNodes.size());

        List<byte[]> res = simpleBTree.query(TypeUtil.intToBytes(keyStart), TypeUtil.intToBytes(keyEnd));
        System.out.println(String.format("expect:%d res:%d start:%d end:%d", keys.size(), res.size(), keyStart, keyEnd));
        boolean succ = true;
        for (byte[] tmp :res){
            int a = (int) TypeUtil.bytesToLong(Arrays.copyOfRange(tmp,0,4));
            System.out.println(String.format("start:%d res:%d end:%d",keyStart, keyEnd, a));
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
