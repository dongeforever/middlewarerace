package com.alibaba.middleware.race.model;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by liuzhendong on 16/7/5.
 */
public class OrderJoinMap {

    public Map<Long,OrderJoinItem> joinMap;


    private JoinMeetListener joinMeetListener;
    private void init(){
        joinMap = new ConcurrentHashMap<Long, OrderJoinItem>(1024);

    }
    public OrderJoinMap(){
        init();
    }
    public void merge(OrderJoinItem item){

        OrderJoinItem oldItem = joinMap.get(item.orderId);
        if(oldItem != null){
            if(oldItem.join(item)){
                joinMap.remove(item.orderId);
                if(joinMeetListener != null) joinMeetListener.onMeet(oldItem);
            }
            return;
        }
        synchronized (this){
            oldItem = joinMap.get(item.orderId);
            if(oldItem != null){
                if(oldItem.join(item)){
                    joinMap.remove(item.orderId);
                    if(joinMeetListener != null) joinMeetListener.onMeet(oldItem);
                }
            }else {
                if(item.from == 0){
                    item.payItems = new ArrayList<>(8);
                    item.payItems.add(new OrderJoinItem(item));
                }
                joinMap.put(item.orderId, item);
            }
        }
    }

    public OrderJoinMap joinMeetListener(JoinMeetListener listener){
        this.joinMeetListener = listener;
        return this;
    }
    public interface  JoinMeetListener{
       void onMeet(OrderJoinItem item);
    }


}
