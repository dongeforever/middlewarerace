package com.alibaba.middleware.race.model;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by liuzhendong on 16/7/5.
 * //需要统计总值,因此需要严格按照时刻表往下统计,否则就会统计不准确
 *
 * //可以抽象一下, 写成一个通用的 movearray
 */
public class PayRatioArray {

    //往前多放5个
    public Long firstKey;
    public int firstIndex;
    public PayRatio[]  ratios;
    public PayRatio[]  ratioTotals;
    public int moveIndex; //已经move到哪个位置了
    public int initIndex; //已经初始化mtime的位置了

    public boolean shutdown;
    private MoveListener moveListener;

    private long moveAfterAccess;
    private Thread moveThread;
    private void init(){
        moveIndex = 0;
        initIndex = -1;
        firstKey = 0L;
        firstIndex = 100;
        ratios = new PayRatio[5000];
        ratioTotals = new PayRatio[5000];
        for (int i = 0; i < ratios.length; i++){
            ratios[i] = new PayRatio();
        }
        moveAfterAccess = 1500;
        shutdown = false;
        moveThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!shutdown){
                    try {
                        long curr  = System.currentTimeMillis();
                        PayRatio ratio = ratios[moveIndex];
                        if(ratio.mtime > 0 && curr - ratio.mtime > moveAfterAccess){
                            ratioTotals[moveIndex] = getTotalRatio(moveIndex);
                            if(moveListener != null) moveListener.onMove(ratioTotals[moveIndex]);
                            //如果该条件不满足,则说明被其它线程修改过,重新再算一遍
                            if(curr - ratio.mtime > moveAfterAccess){
                                moveIndex++;
                            }
                            continue;
                        }
                        Thread.sleep(10);
                    }catch (Exception e){

                    }
                }
            }
        });
        moveThread.start();

    }

    public PayRatio  getTotalRatio(int moveIndex){

        if(moveIndex - 1 < 0){
            return new PayRatio().merge(ratios[moveIndex]);
        }
        Long key = ratios[moveIndex].key;
        return  new PayRatio().key(key).merge(ratioTotals[moveIndex -1]).merge(ratios[moveIndex]);
    }

    public PayRatioArray moveAfterAccess(long duration, TimeUnit timeUnit){
        this.moveAfterAccess = timeUnit.toMillis(duration);
        return this;
    }
    public PayRatioArray(){
        init();
    }

    private void initUntil(int index){
        if(index <= initIndex){
            return;
        }
        synchronized (this){
            if(index <= initIndex){
                return;
            }
            for (int i = initIndex + 1; i <= index;i++){
                ratios[i].merge(new PayRatio());
            }
            initIndex = index;
        }
    }


    private int getIndex(Long key){
        return (int) (firstIndex + (key - firstKey)/60);
    }
    public void ajustMoveIndex(int curr){
        if(curr < moveIndex){
            moveIndex = curr;
        }
    }
    public void merge(PayRatio payRatio){
        if(firstKey == 0L){
            synchronized (this){
                if(firstKey == 0L){
                    ratios[firstIndex].merge(payRatio);
                    firstKey = payRatio.key;
                    initUntil(firstIndex);
                }else {
                    int curr = getIndex(payRatio.key);
                    ratios[curr].merge(payRatio);
                    initUntil(curr);
                    ajustMoveIndex(curr);
                }
            }
        }else {
            int curr = getIndex(payRatio.key);
            if(curr < 0) curr = 0;
            if(curr >= ratios.length) curr = ratios.length - 1;
            ratios[curr].merge(payRatio);
            initUntil(curr);
            ajustMoveIndex(curr);
        }

    }

    public void setMoveListener(MoveListener moveListener){
        this.moveListener = moveListener;
    }

    public interface MoveListener{
       void onMove(PayRatio payRatio);
    }


    public void shutdown(){
        this.shutdown = true;
    }


}
