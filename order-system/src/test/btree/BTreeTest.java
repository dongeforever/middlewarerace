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
public class BTreeTest {

    private Random random = new Random();

    @Test
    public void testBasis()throws IOException{
        BTree bTree = new BTree();
        String key1 = "key001";
        String key2 = "key002";
        List<byte[]> keys = new ArrayList<byte[]>();
        keys.add("key001".getBytes());
        keys.add("key002".getBytes());
        keys.add("key003".getBytes());
        keys.add("key004".getBytes());
        System.out.println(bTree.findChildIndex(keys,key2.getBytes(),6));
    }

    @Test
    public void testInMemory()throws IOException{
        BTree bTree = new BTree();
        bTree.debug = false;
        int num = 5 * 1000* 1000;
        List<String> keys = new ArrayList<String>();
        long start = System.currentTimeMillis();
        for (int i = 0; i < num; i++) {
            String suffix = convert(random.nextInt(num));
            String key = "key" + suffix;
            String value = "value" + suffix;
            //System.out.println(String.format("key:%s value:%s",key,value));
            //keys.add(key);
            bTree.insert(key.getBytes(), value.getBytes());
        }
        long end = System.currentTimeMillis();
        PrintUtil.print("create num:%d cost:%d ms", num, end - start);
        /*
        int testNum=1000,corrNum=0;
        for (int i = 0; i < testNum;i++){
            String key = keys.get(random.nextInt(num));
            List<byte[]> res= bTree.query(key.getBytes());
            boolean correct = true;
            for (int j = 0; j < res.size(); j++) {
                if(!new String(res.get(j)).substring(0,key.length()).equals(key)){
                    correct = false;
                }
            }
            if(correct) corrNum++;
        }
        long end2 = System.currentTimeMillis();
        PrintUtil.print("query testNum:%d corrNum:%d cost:%d ms", testNum, corrNum, end2 - end);
        */
        System.out.println("print the tree=======================================================");
        //printBtreeAsString(bTree);

    }

    @Test
    public void testMulti()throws Exception{
        Runnable task = new Runnable() {
            @Override
            public void run() {
                try {
                    testInMemory();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        };

        int tsNum = 4;
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

    public static void  printBtreeAsString(BTree bTree){
        ArrayDeque<Object> nodes = new ArrayDeque<Object>();
        if(bTree.root == null){
            System.out.println("root is null");
            return;
        }
        nodes.add(bTree.root);
        while (nodes.size() > 0){
            BTree.BNode node = (BTree.BNode) nodes.pop();
            System.out.println(node.keyvalues.size() + "==============================================");
            for (int i = 0; i < node.keyvalues.size(); i++) {
                System.out.println("inner-" + i + ":"+ new String(node.keyvalues.get(i)));
            }
            if(node.childs.get(0) instanceof Long){
                System.out.println("leaf-" + node.childs.size()+"---------------------------------------");
                for (int i = 0; i < node.childs.size(); i++) {
                    Long point = (Long)node.childs.get(i);
                    System.out.println("leaf-" + i + ":"+ point);
                }
            }else {
                nodes.addAll(node.childs);
            }
        }
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
