package com.alibaba.middleware.race.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by liuzhendong on 16/7/6.
 */
public class OrderJoinItem {

    public int from;// 0表示pay,1表示TM,2表示TB
    public final Long orderId;

    public volatile Double totalPrice;

    public volatile Double totalPay;

    public List<OrderJoinItem> payItems;

    public long ctime;

    public OrderJoinItem(Long orderId){
        this(orderId,false);
    }

    public OrderJoinItem(Long orderId,boolean initItems){
        if(initItems){
            payItems = new ArrayList<OrderJoinItem>(8);
        }
        this.orderId = orderId;
        this.totalPrice = 0.0;
        this.totalPay = 0.0;
    }

    public OrderJoinItem(OrderJoinItem item){
        this.orderId = item.orderId;
        this.from = item.from;
        this.totalPay = item.totalPay;
        this.totalPrice = item.totalPrice;
        this.ctime = item.ctime;
    }

    public OrderJoinItem from(int from){
        this.from = from;
        return this;
    }
    public OrderJoinItem totalPrice(double totalPrice){
        this.totalPrice = totalPrice;
        return this;
    }
    public OrderJoinItem totalPay(double totalPay){
        this.totalPay = totalPay;
        return this;
    }
    public OrderJoinItem ctime(long ctime){
        this.ctime = ctime;
        return this;
    }

    public synchronized boolean join(OrderJoinItem orderJoinItem){
        this.totalPay +=  orderJoinItem.totalPay;
        this.totalPrice += orderJoinItem.totalPrice;
        if(orderJoinItem.from > 0){
            this.from = orderJoinItem.from;
        }else {
            if(payItems == null) payItems = new ArrayList<OrderJoinItem>(8);
            payItems.add(orderJoinItem);
        }
        return isMeet();
    }



    public boolean isMeet(){
        return Math.abs(totalPrice - totalPay) < 0.0001;
    }


}
