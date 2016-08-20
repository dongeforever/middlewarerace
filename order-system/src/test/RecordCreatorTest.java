import org.junit.Test;

import java.io.*;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by liuzhendong on 16/7/12.
 */
public class RecordCreatorTest {

    static Random random = new Random();
    static AtomicLong counter = new AtomicLong();
    public final static String GOODS_FORMAT="goodid:%s salerid:%s good_name:%s price:%.2f offprice:%.2f";


    public static String createGoods(){
        String suffix = System.currentTimeMillis()+"_" + counter.addAndGet(1);
        return String.format(GOODS_FORMAT, "test_goodid_"+suffix,
                "test_salerid_" + suffix,
                "test_goodname_" + suffix,
                random.nextInt(1000) * Math.random(),
                random.nextInt(100) * Math.random());
    }

    @Test
    public  void writeGoods()throws Exception{
        int num = 100*1000*1000;
        RandomAccessFile randomAccessFile = new RandomAccessFile("/export/data/order/goods.txt","rw");
        randomAccessFile.seek(randomAccessFile.length());
        randomAccessFile.write("\n".getBytes("utf-8"));
        long start = System.currentTimeMillis();
        /*
        String goods = createGoods();
        for(int i=0; i < num;i++){
            randomAccessFile.write((goods+"\n").getBytes("utf-8"));
            //randomAccessFile.writeChars((createGoods()+"\n"));
        }
        long end = System.currentTimeMillis();
        System.out.println(String.format("lines:%d cost:%dms",num,end-start));
        */
    }



    @Test
    public  void writeGoods2()throws Exception{
        int num = 100*1000*1000;
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("/export/data/order/goods.txt"),true)));
        long start = System.currentTimeMillis();
        for(int i=0; i < num;i++){
            bufferedWriter.write((createGoods()+"\n"));
        }
        long end = System.currentTimeMillis();
        System.out.println(String.format("lines:%d cost:%dms",num,end-start));
    }

    @Test
    public  void read() throws Exception{
        RandomAccessFile randomAccessFile = new RandomAccessFile("/export/data/order/goods.txt","rw");
        for (int i = 0; i < 10; i++) {
            System.out.println((char) randomAccessFile.readByte());
        }
    }

    @Test
    public  void readLine() throws Exception{
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(new File("/export/data/order/goods.txt"))));
        long start = System.currentTimeMillis();
        int num = 0;
        while (true){
            String line = bufferedReader.readLine();
            if(line == null || line.length() ==0){
                break;
            }
            num++;
        }
        long end = System.currentTimeMillis();
        System.out.println(String.format("lines:%d cost:%dms",num,end-start));
    }


    @Test
    public  void testCreateFileNumbers() throws Exception{
        int num = 0;
        for (int i = 0; i < 10000; i++) {
            RandomAccessFile randomAccessFile = new RandomAccessFile("/export/data/order/goods.txt","rw");
            System.out.println(++num);
        }
    }
}
