import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.model.OrderJoinItem;
import com.alibaba.middleware.race.model.OrderJoinMap;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * Created by liuzhendong on 16/7/6.
 */
public class OrderJoinMapTest {
    Random random = new Random();
    OrderJoinMap orderJoinMap = new OrderJoinMap();
    {
        orderJoinMap.joinMeetListener(new OrderJoinMap.JoinMeetListener() {
            @Override
            public void onMeet(OrderJoinItem item) {
                for (OrderJoinItem payItem: item.payItems){
                    //System.out.println(String.format("from:%d ctime:%d amount:%.2f",item.from,payItem.ctime,payItem.totalPay));
                }
            }
        });
    }

    OrderJoinMap orderJoinMap2 = new OrderJoinMap();
    {
        orderJoinMap2.joinMeetListener(new OrderJoinMap.JoinMeetListener() {
            @Override
            public void onMeet(OrderJoinItem item) {
                for (OrderJoinItem payItem: item.payItems){
                    //System.out.println(String.format("from:%d ctime:%d amount:%.2f",item.from,payItem.ctime,payItem.totalPay));
                }
            }
        });
    }

    @Test
    public void join()throws Exception{

        final int num = 1000 * 1000;
        Runnable runnable = new Runnable() {
            @Override
            public void run() {

                for (int i = 0; i < num; i++) {
                    long orderId = random.nextInt(1000);
                    orderJoinMap.merge(new OrderJoinItem(orderId + 1L).totalPrice(10.0).from(1));
                    orderJoinMap.merge(new OrderJoinItem(orderId + 2L).totalPay(2.0).from(0));
                    orderJoinMap.merge(new OrderJoinItem(orderId + 3l).totalPay(8.0).from(0));
                }
            }
        };
        Runnable runnable2 = new Runnable() {
            @Override
            public void run() {

                for (int i = 0; i < num; i++) {
                    long orderId = random.nextInt(1000);
                    orderJoinMap2.merge(new OrderJoinItem(orderId + 1L).totalPrice(10.0).from(1));
                    orderJoinMap2.merge(new OrderJoinItem(orderId + 2L).totalPay(2.0).from(0));
                    orderJoinMap2.merge(new OrderJoinItem(orderId + 3l).totalPay(8.0).from(0));
                }
            }
        };
        Thread[] ts = new Thread[1];
        ts[0] = new Thread(runnable);
        //ts[1] = new Thread(runnable2);
        for (int i = 0; i < ts.length; i++) {
            ts[i].start();
        }
        long start = System.currentTimeMillis();
        for (int i = 0; i < ts.length; i++) {
            ts[i].join();
        }

        long end = System.currentTimeMillis();

        System.out.println(String.format("num:%d remain:%d cost:%d ms", num * ts.length, orderJoinMap.joinMap.size(), end - start));

    }

    @Test
    public void test()throws Exception{
        final Long a = 1l;
        final Long b = 2l;
        Runnable arun = new Runnable() {
            @Override
            public void run() {
                synchronized (a){
                    try {
                        System.out.println("start a");
                        Thread.sleep(20000);
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
                System.out.println("end a");

            }
        };
        Runnable brun = new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(1000);
                }catch (Exception e){
                    e.printStackTrace();
                }
                synchronized (b){
                    try {
                        System.out.println("start b");
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
                System.out.println("end b");
            }
        };
        new Thread(arun).start();
        new Thread(brun).start();
        new CountDownLatch(1).await();

    }

}
