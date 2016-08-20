import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.Tester;
import com.sun.org.apache.xpath.internal.operations.Or;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by liuzhendong on 16/7/28.
 */
public class OrderSystemTest {
    public String baseDir = "/Users/liuzhendong/Source/middleware-race/data/prerun_data/";
    public String storeDir = "/Users/liuzhendong/Source/middleware-race/data";
    List<String> orderFiles = new ArrayList<String>();
    List<String> goodFiles = new ArrayList<String>();
    List<String> buyerFiles = new ArrayList<String>();

    List<String>  storeFolders = new ArrayList<String>();
    {
        orderFiles.add(baseDir +"order.0.0");
        orderFiles.add(baseDir +"order.0.3");
        orderFiles.add(baseDir +"order.1.1");
        orderFiles.add(baseDir +"order.2.2");

        goodFiles.add(baseDir +"good.0.0");
        goodFiles.add(baseDir +"good.1.1");
        goodFiles.add(baseDir +"good.2.2");

        buyerFiles.add(baseDir +"buyer.0.0");
        buyerFiles.add(baseDir +"buyer.1.1");


        storeFolders.add(storeDir+"/index1/");
        storeFolders.add(storeDir+"/index2/");

    }

    @Test
    public void testOrderSystem()throws Exception{
        final OrderSystem orderSystem = new OrderSystemImpl();
        orderSystem.construct(orderFiles, buyerFiles, goodFiles, storeFolders);
        //testQueryOrder(orderSystem);
        //testBuyerTsRange(orderSystem);
        //testSaler(orderSystem);
        //testSumByGood(orderSystem);






        Runnable testTask = new Runnable() {
            @Override
            public void run() {
                try {
                    Tester.test(orderSystem);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        };
        int tsNum = 2;
        Thread[] ts = new Thread[tsNum];
        for (int i = 0; i < ts.length; i++) {
            ts[i] = new Thread(testTask);
        }
        for (int i = 0; i < ts.length; i++) {
            ts[i].start();
        }
        for (int i = 0; i < ts.length; i++) {
            ts[i].join();
        }

    }

    public void testQueryOrder(OrderSystem orderSystem){
        System.out.println(orderSystem.queryOrder(626952019,getKeys("done")));

        //System.out.println(orderSystem.queryOrder(612584687,getKeys("contactphone")));
        //System.out.println(orderSystem.queryOrder(623324410,getKeys("a_b_31770")));
        //System.out.println(orderSystem.queryOrder(589700371,getKeys("a_b_10930")));
    }

    public void testBuyerTsRange(OrderSystem orderSystem){
        Iterator<OrderSystem.Result> resultIterator = orderSystem.queryOrdersByBuyer(1465354705, 1486160674, "ap-8dd6-4aa77f957edf");
        int i = 0;
        while (resultIterator.hasNext()){
            System.out.println(resultIterator.next());
            i++;
        }
        System.out.println("res size:" + i);
    }


    public void testSaler(OrderSystem orderSystem){
        Iterator<OrderSystem.Result> resultIterator = orderSystem.queryOrdersBySaler("ay-a622-39f022439a5e", "dd-892b-e6f4e358d986", null);
        while (resultIterator.hasNext()){
            System.out.println(resultIterator.next());
        }
    }

    public void testSumByGood(OrderSystem orderSystem){
        System.out.println(orderSystem.sumOrdersByGood("aye-a1f0-2fa8c1876590","price"));
        System.out.println(orderSystem.sumOrdersByGood("al-8b8e-fd37a46d8f4e", "a_o_5497"));
    }


    public List<String> getKeys(String... kes){
        List<String> res = new ArrayList<String>();
        for (String key : kes){
            res.add(key);
        }
        return res;
    }
}
