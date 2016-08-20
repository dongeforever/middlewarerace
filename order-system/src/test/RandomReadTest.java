import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.RandomAccessFile;
import java.security.SecureRandom;
import java.util.List;

/**
 * Created by liuzhendong on 16/7/22.
 */
public class RandomReadTest {

    @Test
    public void testRead()throws Exception{
        RandomAccessFile rf = new RandomAccessFile("/Users/liuzhendong/Source/middleware-race/data/prerun_data/case.0","rw");
        int perNum = 1024 * 16;
        byte[] bytes = new byte[1024 * 16];
        int num =0 ;
        long start = System.currentTimeMillis();
        while (true){
            int tmp = rf.read(bytes);
            //System.out.println(String.format("num:%d tmpLen:%d",++num, tmp));
            if(tmp != perNum) break;
        }
        long end = System.currentTimeMillis();
        System.out.println(end-start);
    }


    @Test
    public void testBufferRead()throws Exception{
        BufferedReader br = new BufferedReader(new FileReader(new File("/Users/liuzhendong/Source/middleware-race/data/prerun_data/case.0")));
        long start = System.currentTimeMillis();
        String line = null;
        int num = 0;
        while ((line = br.readLine()) != null){
            num++;
        }
        long end = System.currentTimeMillis();
        System.out.println(end-start +":" + num);
    }
}
