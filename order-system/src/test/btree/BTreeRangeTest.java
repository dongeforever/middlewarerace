package btree;

import com.alibaba.middleware.race.store.BTree;
import com.alibaba.middleware.race.util.PrintUtil;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by liuzhendong on 16/7/19.
 */
public class BTreeRangeTest {

    private Random random = new Random();


    @Test
    public void testRangeQuery()throws IOException{
        BTree bTree = new BTree();
        bTree.debug = false;
        int num = 20000;
        List<String> keys = new ArrayList<String>();
        long start = System.currentTimeMillis();
        for (int i = 0; i < num; i++) {
            String suffix = convert(random.nextInt(num));
            String key = "key"+suffix;
            String value = "value"+suffix;
            //System.out.println(String.format("key:%s value:%s",key,value));
            keys.add(key);
            bTree.insert(key.getBytes(), value.getBytes());
        }
        int testNum = 10000,correctNum = 0;
        for (int i = 0;i < testNum;i++){
            int begin = random.nextInt(num);
            int end = begin + random.nextInt(100);
            String from = "key" + convert(begin);
            String to = "key"+convert(end);
            List<byte[]> res= bTree.query(from.getBytes(), to.getBytes());
            if(res.size() == 0){
                correctNum++;
                continue;
            }
            boolean correct = true;
            String last = new String(res.get(0));
            for (int j = 0; j < res.size(); j++) {
                String  curr = new String(res.get(j));
                if(curr.compareTo(last) <0){
                    correct = false;
                }
                //PrintUtil.print(curr);
            }
            System.out.println(correct);
            if(correct) correctNum++;
        }
        PrintUtil.print("testNum:%d corrNum:%d", testNum, correctNum);
        //printBtreeAsString(bTree);

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
