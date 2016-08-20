package com.alibaba.middleware.race.store;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;

/**
 * Created by liuzhendong on 16/8/2.
 * 堆外内存的受gc影响,释放不可控,这里自己动手管理
 */
public class DirectBuffMgr {


    public int maxNum;
    ArrayDeque<ByteBuffer> buffStack;
    public DirectBuffMgr(int num){
        this.maxNum = num;
        buffStack = new ArrayDeque<ByteBuffer>(num);
    }

    public void push(ByteBuffer byteBuffer){
        byteBuffer.clear();
        buffStack.push(byteBuffer);
    }
    public ByteBuffer allocate(int capacity){
        int remain = buffStack.size();
        while (remain > 0){
            ByteBuffer buffer = buffStack.pop();
            if(buffer.capacity() == capacity){
                return buffer;
            }else {
                remain--;
                buffStack.add(buffer);
            }
        }
        return ByteBuffer.allocateDirect(capacity);
    }
}
