package table;

import com.alibaba.middleware.race.store.SimpleBTree;
import com.alibaba.middleware.race.table.BuyerTable;
import com.alibaba.middleware.race.table.GoodTable;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by liuzhendong on 16/7/28.
 */
public class BuyerTableTest {

    public String baseDir = "/Users/liuzhendong/Source/middleware-race/data/prerun_data/";
    public String storeDir = "/Users/liuzhendong/Source/middleware-race/data";
    List<String> buyerFiles = new ArrayList<String>();
    List<String>  storeFolders = new ArrayList<String>();
    {
        buyerFiles.add(baseDir +"buyer.0.0");
        buyerFiles.add(baseDir +"buyer.1.1");

        storeFolders.add(storeDir+"/index1/");
        storeFolders.add(storeDir+"/index2/");

    }


    @Test
    public void testBuildGoodTable()throws Exception{
        BuyerTable buyerTable = new BuyerTable(buyerFiles, storeFolders);
        long start = System.currentTimeMillis();
        buyerTable.buildIndex();
        long end = System.currentTimeMillis();
        System.out.println(String.format("cost:%d ms", end - start));

        int kvNum = 0;
        for (SimpleBTree sim : buyerTable.bTreesById){
            kvNum += sim.kvNum;
        }
        System.out.println(kvNum);

        Map<String,String> res = buyerTable.getBuyerById("wx-a6c3-86a6eba4d210");
        for (String key : res.keySet()){
            System.out.println(String.format("key:%s value:%s", key, res.get(key)));
        }
    }
}
