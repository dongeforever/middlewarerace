package table;

import com.alibaba.middleware.race.model.Constant;
import com.alibaba.middleware.race.store.SimpleBTree;
import com.alibaba.middleware.race.table.OrderTable;
import com.sun.tools.javac.api.ClientCodeWrapper;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by liuzhendong on 16/7/27.
 */
public class OrderTableTest {

    public String orderDir = "/Users/liuzhendong/Source/middleware-race/data/prerun_data/";
    public String storeDir = "/Users/liuzhendong/Source/middleware-race/data";
    List<String>  orderFiles = new ArrayList<String>();
    List<String>  storeFolders = new ArrayList<String>();
    {
        orderFiles.add(orderDir+"order.0.0");
        orderFiles.add(orderDir+"order.0.3");
        orderFiles.add(orderDir+"order.1.1");
        orderFiles.add(orderDir+"order.2.2");

        storeFolders.add(storeDir+"/index1/");
        storeFolders.add(storeDir+"/index2/");

    }

    @Test
    public void testBuildOrderTable()throws Exception{
        OrderTable orderTable = new OrderTable(orderFiles, storeFolders);
        long start = System.currentTimeMillis();
        orderTable.buildIndex();
        long end = System.currentTimeMillis();
        System.out.println(String.format("cost:%d ms", end - start));

        int kvNum = 0;
        for (SimpleBTree sim : orderTable.bTreesById){
            kvNum += sim.kvNum;
        }
        System.out.println(kvNum);

        queryOrderById(orderTable);

        queryOrderByBuyer(orderTable);

        queryOrderGood(orderTable);

    }


    private void queryOrderById(OrderTable orderTable)throws Exception{
        long start = System.currentTimeMillis();
        for (int i = 0;i < 1000; i++){
            Map<String,String> res = orderTable.getOrderById(605653189l);
            //System.out.println(res);
        }
        long end = System.currentTimeMillis();
        System.out.println(String.format("queryOrderById cost:%d", end-start));
    }

    private void queryOrderByBuyer(OrderTable orderTable)throws Exception{
        long start = System.currentTimeMillis();
        List<Map<String,String>> res = orderTable.getOrdersByBuyer("ap-992a-3341260aa01b", 0, 1468853174);
        for (Map<String , String> row : res){
            System.out.println(String.format("ctime:%s orderId:%s  buyerId:%s",
                    row.get(Constant.CTIME),
                    row.get(Constant.ORDER_ID),
                    row.get(Constant.BUYER_ID)));
        }
        long end = System.currentTimeMillis();
        System.out.println(String.format("queryOrderByBuyer size:%d cost:%d",res.size(), end-start));
    }



    private void queryOrderGood(OrderTable orderTable)throws Exception{
        long start = System.currentTimeMillis();
        List<Map<String,String>> res = orderTable.getOrdersByGood("dd-b00a-d67c9f59ce06");
        for (Map<String , String> row : res){
            System.out.println(String.format("orderId:%s  goodid:%s",
                    row.get(Constant.ORDER_ID),
                    row.get(Constant.GOOD_ID)));
        }
        long end = System.currentTimeMillis();
        System.out.println(String.format("queryOrderGood size:%d cost:%d",res.size(), end-start));
    }



}
