package com.alibaba.middleware.race.util;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by liuzhendong on 16/7/19.
 */
public class OrderUtil {
    public static int hash(String id){
        int h = Arrays.hashCode(id.getBytes());
        if(h < 0) h = h >>> 1;
        return h;
    }

    public static Map<String,String> parse(String line){

        String[]  splits = line.split("\t");
        Map<String,String> fields = new HashMap<String, String>();
        for (String split:splits){
            if(split.length() == 0) continue;
            int index = split.indexOf((int)':');
            if(index == -1) continue;
            fields.put(split.substring(0,index),split.substring(index+1));
        }
        return fields;
    }

    public static String readLine(RandomAccessFile accessFile,long pos,int size)throws IOException{
        byte[] buff = new byte[size];
        accessFile.seek(pos);
        ByteArrayOutputStream out = new ByteArrayOutputStream(size);
        while (true){
            int readNum = accessFile.read(buff);
            for (int i = 0; i < readNum; i++) {
               if(buff[i] == '\n'){
                   out.write(buff,0,i);
                   return new String(out.toByteArray());
               }
            }
            out.write(buff,0,readNum);
        }
    }

    public static void filter(Map<String,String> des, Map<String,String> src, Collection<String> keys){
        for (String key : keys){
            if(src.containsKey(key)){
                des.put(key, src.get(key));
            }
        }
    }

    public static String makeGoodId(String goodId){
        if(goodId.length() == 21){
            return goodId;
        }
        return goodId+" ";
    }
}
