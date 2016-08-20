package com.alibaba.middleware.race.model;

/**
 * Created by liuzhendong on 16/7/5.
 */
public class PayRatio {

    public Long key;
    public double wxPay;
    public double pcPay;
    public volatile long mtime;

    public synchronized PayRatio merge(PayRatio payRatio){
        if(key == null) this.key = payRatio.key;
        this.wxPay += payRatio.wxPay;
        this.pcPay += payRatio.pcPay;
        this.mtime = System.currentTimeMillis();
        return this;
    }

    public PayRatio key(Long key){
        this.key = key;
        return this;
    }
    public PayRatio wxPay(double wxPay){
        this.wxPay = wxPay;
        return this;
    }
    public PayRatio pcPay(double pcPay){
        this.pcPay = pcPay;
        return this;
    }
}
