package com.alibaba.middleware.race.model;

import java.util.concurrent.TimeUnit;

/**
 * Created by liuzhendong on 16/7/5.
 * //按时刻表走,保证按顺序计算
 * //可以抽象一下, 写成一个通用的 movearray
 */
public class ItemArray {

    //往前多放5个
    public Long firstKey;
    public int firstIndex;
    public Item[]  ratios;
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
        ratios = new Item[5000];
        for (int i = 0; i < ratios.length; i++){
            ratios[i] = new Item();
        }
        moveAfterAccess = 1500;
        shutdown = false;
        moveThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!shutdown){
                    try {
                        long curr  = System.currentTimeMillis();
                        Item ratio = ratios[moveIndex];
                        if(ratio.mtime > 0 && curr - ratio.mtime > moveAfterAccess){
                            if(moveListener != null) moveListener.onMove(ratios[moveIndex]);
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

    public ItemArray moveAfterAccess(long duration, TimeUnit timeUnit){
        this.moveAfterAccess = timeUnit.toMillis(duration);
        return this;
    }
    public ItemArray(){
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
                ratios[i].merge(new Item());
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
    public void merge(Item item){
        if(firstKey == 0L){
            synchronized (this){
                if(firstKey == 0L){
                    ratios[firstIndex].merge(item);
                    firstKey = item.key;
                    initUntil(firstIndex);
                }else {
                    int curr = getIndex(item.key);
                    ratios[curr].merge(item);
                    initUntil(curr);
                    ajustMoveIndex(curr);
                }
            }
        }else {
            int curr = getIndex(item.key);
            if(curr < 0) curr = 0;
            if(curr >= ratios.length) curr = ratios.length - 1;
            ratios[curr].merge(item);
            initUntil(curr);
            ajustMoveIndex(curr);
        }

    }

    public void setMoveListener(MoveListener moveListener){
        this.moveListener = moveListener;
    }

    public interface MoveListener{
       void onMove(Item item);
    }


    public void shutdown(){
        this.shutdown = true;
    }


}
