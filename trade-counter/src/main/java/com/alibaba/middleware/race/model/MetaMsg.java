package com.alibaba.middleware.race.model;

/**
 * Created by liuzhendong on 16/7/10.
 */
public class MetaMsg {
    public int topic;
    public byte[] body;

    public MetaMsg topic(int topic){
        this.topic = topic;
        return this;
    }
    public MetaMsg body(byte[] body){
        this.body = body;
        return this;
    }

}
