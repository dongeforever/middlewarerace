package com.alibaba.middleware.race.model;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by liuzhendong on 16/7/5.
 */
public class ItemMap {

    public Map<Long,Item> itemMap;

    private RemoveListener removeListener;
    private long expireAfterAccess;
    private Thread removeThread;
    private boolean shutdown;
    private void init(){
        itemMap = new ConcurrentHashMap<Long, Item>();
        expireAfterAccess = 5000;

    }
    public ItemMap(){
        init();
        shutdown = false;
        removeThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!shutdown){
                    long curr = System.currentTimeMillis();
                    Iterator<Long>  it = itemMap.keySet().iterator();
                    while (it.hasNext()){
                        Long key = it.next();
                        Item item = itemMap.get(key);
                        if(curr - item.mtime > expireAfterAccess){
                            if(removeListener != null){
                                removeListener.onRemove(key,item);
                            }
                            if(curr - item.mtime > expireAfterAccess){
                                it.remove();
                            }
                        }
                    }
                    try {
                        Thread.sleep(1000);
                    }catch (Exception e){

                    }
                }
            }
        });
        removeThread.start();
    }

    public void expireAfterAccess(long duration, TimeUnit timeUnit){
        expireAfterAccess = timeUnit.toMillis(duration);
    }
    public void merge(Item item){
        Item oldItem = itemMap.get(item.key);
        if(oldItem != null){
            oldItem.calculate(item);
            return;
        }
        synchronized (this){
            oldItem = itemMap.get(item.key);
            if(oldItem != null){
                oldItem.calculate(item);
            }else {
                itemMap.put(item.key, item);
            }
        }
    }

    public void setRemoveListener(RemoveListener removeListener){
        this.removeListener = removeListener;
    }

    public interface RemoveListener{
       void onRemove(Long key,Item item);
    }


    public void shutdown(){
        this.shutdown = true;
    }

}
