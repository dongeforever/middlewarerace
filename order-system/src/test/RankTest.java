import com.alibaba.middleware.race.util.RankUtil;
import com.alibaba.middleware.race.util.TypeUtil;
import org.junit.Test;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * Created by liuzhendong on 16/7/26.
 */
public class RankTest {

    public Random random = new Random();
    @Test
    public void testRankList()throws Exception{
        int num = 1024* 1024 * 10;
        List<byte[]>  keys = new ArrayList<byte[]>(num);
        for (int i = 0; i < num; i++) {
            keys.add(TypeUtil.longToBytes(random.nextLong()));
        }
        long start = System.currentTimeMillis();
        Collections.sort(keys, new Comparator<byte[]>() {
            @Override
            public int compare(byte[] o1, byte[] o2) {
                return RankUtil.compare(o1,o2,8);
            }
        });
        ByteBuffer byteBuffer = ByteBuffer.allocate(num*8);
        for (byte[] bytes : keys){
            byteBuffer.put(bytes);
        }
        byteBuffer.flip();
        RandomAccessFile randomAccessFile = new RandomAccessFile("/Users/liuzhendong/Source/middleware-race/data/index/test.index.b","rw");
        FileChannel fileChannel =  randomAccessFile.getChannel();
        fileChannel.write(byteBuffer);
        fileChannel.close();
        long end = System.currentTimeMillis();
        System.out.println(String.format("size:%d cost:%d ms", keys.size(),end - start));
    }

    public void testMerge()throws Exception{
        long start = System.currentTimeMillis();
        ByteBuffer  buffA = ByteBuffer.allocate(16 * 1024);
        ByteBuffer  buffB = ByteBuffer.allocate(16 * 1024);
        ByteBuffer  buffC = ByteBuffer.allocate(16 * 1024);

        FileChannel channelA = new RandomAccessFile("/Users/liuzhendong/Source/middleware-race/data/index/test.index.a","r").getChannel();
        FileChannel channelB = new RandomAccessFile("/Users/liuzhendong/Source/middleware-race/data/index/test.index.b","r").getChannel();
        FileChannel channelC = new RandomAccessFile("/Users/liuzhendong/Source/middleware-race/data/index/test.index.c","r").getChannel();

        channelA.read(buffA);
        channelB.read(buffB);
        boolean lastInsertIsA = false;
        byte[] a = new byte[8];
        byte[] b = new byte[8];
        buffA.get(a);
        buffB.get(b);
        if(RankUtil.compare(a,b,a.length) < 0){
            buffC.put(a);
            lastInsertIsA = true;
        }else {
            buffC.put(b);
            lastInsertIsA = false;
        }
        while (true){
            int readA = 0,readB = 0;
            if(buffA.remaining() == 0){
                buffA.clear();
                readA = channelA.read(buffA);
                buffA.flip();
            }
            if(buffB.remaining() == 0){
                buffB.clear();
                readB = channelB.read(buffB);
                buffB.flip();
            }

            buffA.get(a);

        }


    }
    @Test
    public void testRankArray(){
        int num = 1024* 1024 * 10;
        byte[][]  keys = new byte[num][];
        for (int i = 0; i < num; i++) {
            keys[i] = TypeUtil.longToBytes(random.nextLong());
        }
        long start = System.currentTimeMillis();
        Arrays.sort(keys,new Comparator<byte[]>(){
            @Override
            public int compare(byte[] o1, byte[] o2) {
                return RankUtil.compare(o1,o2,8);
            }
        });
        long end = System.currentTimeMillis();
        System.out.println(String.format("size:%d cost:%d ms", keys.length,end - start));
    }
}
