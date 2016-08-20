package com.alibaba.middleware.race;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;


public class RaceUtils {
    /**
     * 由于我们是将消息进行Kryo序列化后，堆积到RocketMq，所有选手需要从metaQ获取消息，
     * 反序列出消息模型，只要消息模型的定义类似于OrderMessage和PaymentMessage即可
     * @param object
     * @return
     */

    public static ThreadLocal<Kryo>  kryos = new ThreadLocal<Kryo>(){
        @Override
        protected Kryo initialValue(){
            return new Kryo();
        }
    };
    public static byte[] writeKryoObject(Object object) {
        Output output = new Output(1024);
        kryos.get().writeObject(output, object);
        output.flush();
        output.close();
        byte [] ret = output.toBytes();
        output.clear();
        return ret;
    }

    public static <T> T readKryoObject(Class<T> tClass, byte[] bytes) {

        Input input = new Input(bytes);
        input.close();
        T ret = kryos.get().readObject(input, tClass);
        return ret;
    }

    public static Long getMinuteTime(long ctime){
        return (ctime / 1000 / 60) * 60;
    }

    public static double round(double origin){
        long tmp = Math.round(origin * 100);
        return  (double)tmp/100;
    }

}
