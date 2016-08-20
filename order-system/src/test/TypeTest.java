import com.alibaba.middleware.race.util.TypeUtil;
import org.junit.Test;

/**
 * Created by liuzhendong on 16/7/18.
 */
public class TypeTest {

    @Test
    public void test(){
        System.out.println(TypeUtil.bytesToLong(TypeUtil.longToBytes(2142343231)));
    }

    @Test
    public void testByte(){
        System.out.println((short)0xFFFFFF);
        System.out.println(Long.valueOf(0xFFFFFFl).shortValue());
        byte[] tmp = TypeUtil.shortToBytes((short)(0x1F | 0x8F));
        for (byte b:tmp){
            System.out.println(b);
        }
    }


    @Test
    public void testBit(){
        byte a = (byte)23;
        byte b = (byte)(a | 0x80);
        byte c = (byte) (b & 0x7F);
        System.out.println(b);
        System.out.println(c);
        System.out.println(((byte)123  & 0x80) != 0);
        System.out.println(Boolean.valueOf("true"));
        System.out.println(Math.pow(3,0.7));

        System.out.println(3010.3 * Math.log(1947) /(1* Math.pow(4,0.7)));
    }


    @Test
    public void testNewLine(){
        System.out.println((int)'\n');
    }

    @Test
    public void testPow(){
        System.out.println((long) 1 << 56);
        System.out.println(Long.MAX_VALUE);

        System.out.println(Long.parseLong("123"));
    }
}
