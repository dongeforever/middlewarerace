package com.alibaba.middleware.race.store;

import java.util.List;

/**
 * Created by liuzhendong on 16/7/28.
 */
public class FlushHelper {
    public static final int CON_NUM = 2;
    static class FlushTask implements Runnable{

        public int id;
        public List<SimpleBTree> bTrees;
        public FlushTask(int id, List<SimpleBTree> bTrees){
            this.id = id;
            this.bTrees = bTrees;
        }
        @Override
        public void run() {
            try {
                for (int i = 0; i < bTrees.size(); i++) {
                    if (i % CON_NUM != this.id) {
                        continue;
                    }
                    bTrees.get(i).reRank();
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }


    public static void  flush(List<SimpleBTree> btrees)throws InterruptedException{
        Thread[] ts = new Thread[CON_NUM];
        for (int i = 0; i < ts.length; i++) {
            ts[i] = new Thread(new FlushTask(i, btrees));
        }
        for (int i = 0; i < ts.length; i++) {
            ts[i].start();
        }
        for (int i = 0; i < ts.length; i++) {
            ts[i].join();
        }
    }
}
