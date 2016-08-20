import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.*;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by liuzhendong on 16/7/6.
 */
public class ItemMapTest {

    Random random = new Random();

    ItemMap itemMap  = new ItemMap();
    {
        itemMap.expireAfterAccess(1, TimeUnit.SECONDS);

        itemMap.setRemoveListener(new ItemMap.RemoveListener() {
            @Override
            public void onRemove(Long key, Item item) {
                System.out.println(String.format("key:%d amount:%.2f remain:%d", item.key, item.amount,itemMap.itemMap.size()));
            }
        });
    }


    @Test
    public void add()throws Exception{
        Long base = System.currentTimeMillis();
        int num = 1000 * 1000;
        long start =  System.currentTimeMillis();
        for(int i = 0; i < num;i++){
            long ctime = base + random.nextInt(6000 * 1000);
            Item item = new Item().key(RaceUtils.getMinuteTime(ctime)).amount(2.0);
            itemMap.merge(item);
        }
        long end = System.currentTimeMillis();
        System.out.println(String.format("num:%d cost:%d ms",num, end -start));
        Thread.sleep(20 * 1000);
    }



    @Test
    public void round(){
        System.out.println(RaceUtils.round(13.4444));
        System.out.println(RaceUtils.round(13.4454));
        System.out.println(RaceUtils.round(13.4464));

        System.out.println(TableItemFactory.round(13.4454,2));
        System.out.println(TableItemFactory.round(13.4444,2));

        System.out.println(RaceUtils.getMinuteTime(1468044893356l));
    }




}
