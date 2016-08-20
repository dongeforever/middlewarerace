package table;

import com.alibaba.middleware.race.store.SimpleBTree;
import com.alibaba.middleware.race.table.GoodTable;
import com.alibaba.middleware.race.table.OrderTable;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by liuzhendong on 16/7/28.
 */
public class GoodTableTest {

    public String baseDir = "/Users/liuzhendong/Source/middleware-race/data/prerun_data/";
    public String storeDir = "/Users/liuzhendong/Source/middleware-race/data";
    List<String> goodFiles = new ArrayList<String>();
    List<String>  storeFolders = new ArrayList<String>();
    {
        goodFiles.add(baseDir +"good.0.0");
        goodFiles.add(baseDir +"good.1.1");
        goodFiles.add(baseDir +"good.2.2");

        storeFolders.add(storeDir+"/index1/");
        storeFolders.add(storeDir+"/index2/");

    }


    @Test
    public void testBuildGoodTable()throws Exception{
        GoodTable goodTable = new GoodTable(goodFiles, storeFolders);
        long start = System.currentTimeMillis();
        goodTable.buildIndex();
        long end = System.currentTimeMillis();
        System.out.println(String.format("cost:%d ms", end - start));

        int kvNum = 0;
        for (SimpleBTree sim : goodTable.bTreesById){
            kvNum += sim.kvNum;
        }
        System.out.println(kvNum);

        Map<String,String> res = goodTable.getGoodById("al-a0c0-77686a3c8e11");
        for (String key : res.keySet()){
            System.out.println(String.format("key:%s value:%s", key, res.get(key)));
        }

    }
}
