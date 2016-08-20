package com.alibaba.middleware.race.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by liuzhendong on 16/7/26.
 */
public class RankUtil {
    public static int compare(byte[] key1,byte[] key2,int len){
        for (int i = 0; i < len; i++) {
            if((key1[i] & 0xFF) < (key2[i] & 0xFF)) return -1;
            if((key1[i] & 0XFF) > (key2[i] & 0xFF)) return 1;
        }
        return 0;
    }

    public static int findChildIndex(List<byte[]> keyvalues, byte[] key, int keySize){
        int start = 0,end = keyvalues.size()-1;
        while (true){
            try {
                if(start == end){
                    int res = RankUtil.compare(key,keyvalues.get(start),keySize);
                    return  res <= 0 ? start : start + 1;
                }
                int mid = (start+end)/2;
                int res = RankUtil.compare(key, keyvalues.get(mid), keySize);
                if(res == 0) return mid;

                if(res < 0){
                    end = mid; //注意不是mid -1
                }else {
                    start = mid + 1; //注意是mid+1
                }
            }catch (Exception e){
                PrintUtil.print("start:%d end:%d size:%d", start, end, keyvalues.size());
                e.printStackTrace();
                System.exit(1);
            }

        }
    }

    public static List<byte[]> getResultByKey(List<byte[]> kvs, byte[] key){
        int index = RankUtil.findChildIndex(kvs, key, key.length);
        if(index >= kvs.size()){
            return new ArrayList<byte[]>(4);
        }
        List<byte[]> result = new ArrayList<byte[]>();
        //考虑到key会重复,所以往前往后找
        for (int i = index; i < kvs.size(); i++) {
            if(RankUtil.compare(kvs.get(i),key,key.length) == 0){
                result.add(kvs.get(i));
            }else {
                break;
            }
        }
        for (int i = index-1; i >= 0; i--) {
            if(RankUtil.compare(kvs.get(i),key,key.length) == 0){
                result.add(kvs.get(i));
            }else {
                break;
            }
        }
        return result;
    }

    //注意 这里给的是视图
    public static List<byte[]> getResultByRange(List<byte[]> kvs, byte[] from, byte[] to){
        int start = 0, end = kvs.size()-1;
        if(from != null){
            start = findChildIndex(kvs,from,from.length);
            //start校验
            if(start == kvs.size()){
                return new ArrayList<byte[]>(4);
            }
            if(compare(from, kvs.get(start),from.length) == 0){
                start--;
                while (start >= 0){
                    if(compare(from,kvs.get(start),from.length) == 0){
                        start--;
                    }else {
                        break;
                    }
                }
                start++;
            }
        }

        if(to != null){
            end = findChildIndex(kvs, to ,to.length);
            if(end == kvs.size()){
                end--;
            }else if(compare(to, kvs.get(end),to.length) == 0){
                end--;
                while (end >= 0){
                    if(compare(to, kvs.get(end),to.length) == 0){
                        end--;
                    }else {
                        break;
                    }
                }
            }else {
                end --;
            }
        }

        if(start > end){
            return new ArrayList<byte[]>(4);
        }
        return kvs.subList(start,end+1);
    }
}
