import com.alibaba.middleware.race.util.OrderUtil;
import com.alibaba.middleware.race.util.RankUtil;
import com.alibaba.middleware.race.util.TypeUtil;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;

/**
 * Created by liuzhendong on 16/7/20.
 */
public class UtilTest {

    @Test
    public void testParse(){
        Map fields = OrderUtil.parse("1:12\t2:23\t312\t");
        System.out.println(fields);
    }


    @Test
    public void testHash(){
        System.out.println(Arrays.hashCode("dd-b00a-d67c9f59ce06".getBytes()));
        System.out.println(OrderUtil.hash("dd-b00a-d67c9f59ce06"));

    }

    Random random = new Random();
    @Test
    public void testCmp(){
        int num = 10000;
        for (int i = 0; i < num; i++) {
            Long a = random.nextLong();
            Long b = random.nextLong();
            if(a < 0) a= a >>> 1;
            if(b < 0) b = b >>>1;
            if(a.compareTo(b) != RankUtil.compare(TypeUtil.longToBytes(a), TypeUtil.longToBytes(b), 8)){
                System.out.println(String.format("%d : %d",a,b));
            }
        }
    }

}
