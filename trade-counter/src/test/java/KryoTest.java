import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.PaymentMessage;
import org.junit.Test;

/**
 * Created by liuzhendong on 16/7/9.
 */
public class KryoTest {


    @Test
    public void testDeserialize()throws Exception{
        PaymentMessage paymentMessage = new PaymentMessage();
        paymentMessage.setOrderId(1);
        paymentMessage.setCreateTime(System.currentTimeMillis());
        paymentMessage.setPayAmount(1111);
        paymentMessage.setPaySource((short)1);
        paymentMessage.setOrderId(paymentMessage.getOrderId()+1);
        final  byte[] bytes = RaceUtils.writeKryoObject(paymentMessage);
        final int num = 1000 * 1000;
        long start = System.currentTimeMillis();
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < num; i++) {
                    PaymentMessage tmp = RaceUtils.readKryoObject(PaymentMessage.class,bytes);
                    //System.out.println(tmp);
                }
            }
        };

        Thread[] ts = new Thread[5];
        for (int i = 0; i < ts.length; i++) {
            ts[i] = new Thread(runnable);
        }
        for (int i = 0; i < ts.length; i++) {
            ts[i].start();
        }
        for (int i = 0; i < ts.length; i++) {
            ts[i].join();
        }
        long end = System.currentTimeMillis();
        System.out.println(String.format("num:%d cost:%d ms",num * ts.length,end - start));
    }
}
