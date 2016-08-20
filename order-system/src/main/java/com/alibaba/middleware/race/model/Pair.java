package com.alibaba.middleware.race.model;

/**
 * Created by liuzhendong on 16/7/25.
 */
public class Pair {
    public byte[] key;
    public byte[] value;
    public Pair key(byte[] key){
        this.key = key;
        return this;
    }
    public Pair value(byte[] value){
        this.value = value;
        return this;
    }
}
