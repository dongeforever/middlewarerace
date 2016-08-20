import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.PayRatio;
import com.alibaba.middleware.race.model.PayRatioArray;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by liuzhendong on 16/7/6.
 */
public class PayRatioArrayTest {

    Random random = new Random();
    PayRatioArray payRatioArray  = new PayRatioArray();
    {
        payRatioArray.moveAfterAccess(1, TimeUnit.SECONDS);

        payRatioArray.setMoveListener(new PayRatioArray.MoveListener() {
            @Override
            public void onMove(PayRatio payRatio) {
                System.out.println(String.format("move key:%d moveIndex:%d wx:%.2f pc:%.2f",payRatio.key, payRatioArray.moveIndex,
                        payRatio.wxPay, payRatio.pcPay));
            }
        });
    }


    @Test
    public void add()throws Exception{
        Long base = System.currentTimeMillis();

        long start = System.currentTimeMillis();
        int num = 1000000;
        for(int i = 0; i < num;i++){
            long ctime = base + random.nextInt(6000 * 1000);
            PayRatio payRatio = new PayRatio().key(RaceUtils.getMinuteTime(ctime)).pcPay(1.0).wxPay(2.0);
            payRatioArray.merge(payRatio);
        }
        long end = System.currentTimeMillis();
        System.out.println(String.format("num:%d cost:%d ms",num, end-start));
        Thread.sleep(20 * 1000);
    }




}
