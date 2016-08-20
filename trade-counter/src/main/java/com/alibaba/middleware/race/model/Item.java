package com.alibaba.middleware.race.model;

/**
 * Created by liuzhendong on 16/7/5.
 */
public class Item {

    public Long key;
    public double amount;
    public long ctime;
    public long mtime;


    public Item(){
        mtime = System.currentTimeMillis();
    }

    public Item key(Long key){
        this.key = key;
        return this;
    }
    public Item amount(double amount){
        this.amount = amount;
        return this;
    }

    public Item ctime(long ctime){
        this.ctime = ctime;
        return this;
    }

    public synchronized Item calculate(Item item){
        this.amount += item.amount;
        this.mtime = System.currentTimeMillis();
        return this;
    }

    public synchronized Item merge(Item item){
        if(this.key == null) this.key = item.key;
        this.amount += item.amount;
        this.mtime = System.currentTimeMillis();
        return this;
    }
}
