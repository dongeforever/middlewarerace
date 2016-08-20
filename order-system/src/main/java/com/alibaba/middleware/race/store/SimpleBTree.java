package com.alibaba.middleware.race.store;

import com.alibaba.middleware.race.util.PrintUtil;
import com.alibaba.middleware.race.util.RankUtil;
import com.alibaba.middleware.race.util.TypeUtil;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by liuzhendong on 16/7/27.
 * 简单的B树,约定如下:
 * 1 每颗B树可以完整加载到内存中
 * 2 每棵B树只能构建一次,中途插入的成本很高
 * 3 先按插入顺序存储到文件中,待插入结束,从文件中读出所有数据在内存中排序后再插入
 *
 */
public class SimpleBTree {

    public class Node{
        byte[] key;
        int pos;
        public Node(){}
        public Node(byte[] key,int pos){
            this.key = key;
            this.pos = pos;
        }
    }
    public boolean cache = false;
    public AtomicInteger queryNum = new AtomicInteger(0);
    public AtomicInteger hitCacheNum = new AtomicInteger(0);
    public String name = "NONE";
    public SimpleBTree name(String name){
        this.name = name;
        return this;
    }

    final DirectBuffMgr  directBuffMgr  = new DirectBuffMgr(400);
    //TODO 设计一个600大小的缓存,以direct buff 的形式存储
    LruCache<String,ByteBuffer> buffCache = new LruCache<String, ByteBuffer>(400,400,"stree"){

        @Override
        protected boolean removeEldestEntry(Map.Entry<String,ByteBuffer> eldest) {
            if(super.size() > 400){
                directBuffMgr.push(eldest.getValue());
                return true;
            }
            return false;
        }
    };

    public String filePath;
    public FileChannel fileChannel;
    public int keySize;
    public int dataSize;
    public ByteBuffer byteBuffer;
    public int kvNum;
    public List<Node>  keyNodes; //类似于B树的内节点
    public SimpleBTree(String filePath,int keySize,int dataSize,boolean cache)throws IOException{
        this.filePath = filePath;
        this.keySize = keySize;
        this.dataSize = dataSize;
        fileChannel = new RandomAccessFile(filePath,"rw").getChannel();
        byteBuffer = ByteBuffer.allocate(StoreConfig.DEFAULT_BUFF_LEN);
        keyNodes = new ArrayList<Node>(10 * 1024);
        this.cache = cache;
    }

    public synchronized void write(byte[] keyvalue)throws IOException{
        if(byteBuffer.remaining() < keyvalue.length){
            byteBuffer.flip();
            fileChannel.write(byteBuffer);
            byteBuffer.clear();
        }
        byteBuffer.put(keyvalue);
        kvNum++;
    }

    public void finishWrite()throws IOException{
        byteBuffer.flip();
        fileChannel.write(byteBuffer);
        byteBuffer.clear();
    }

    public List<byte[]> getValuesByPosRange(long posStart, long posEnd)throws IOException{
        if(cache){
            //TODO 有bug 大小不一致的
            int qNum = queryNum.addAndGet(1);
            int hNum = hitCacheNum.get();
            String key = posStart+":" + posEnd;
            ByteBuffer buff = buffCache.get(key);
            if(buff == null){
                synchronized (fileChannel){
                    buff = directBuffMgr.allocate((int) (posEnd-posStart));
                    fileChannel.position(posStart);
                    fileChannel.read(buff);
                }
                buff.flip();
                buffCache.put(key,buff);
            }else {
                hNum = hitCacheNum.addAndGet(1);
            }
            if(qNum % 1000 == 0){
                PrintUtil.print("SIMTREE %s CACHE HIT %d %d %.2f", this.name, qNum, hNum, (hNum+0.1)/qNum);
            }
            synchronized (buff){
                buff.rewind();
                return deserialize(buff, buff.remaining());
            }
        }else {
            ByteBuffer buff = ByteBuffer.allocate((int) (posEnd-posStart));
            synchronized (fileChannel){
                fileChannel.position(posStart);
                fileChannel.read(buff);
            }
            buff.flip();
            List<byte[]> res = deserialize(buff,buff.remaining());
            buff.clear();
            return res;
        }
    }

    public List<byte[]> query(byte[] key)throws IOException{
        checkReRank();
        if(key.length != keySize){
            throw new RuntimeException(String.format("key len is not consistent %d != %d",key.length, keySize));
        }

        int leafIndex = findChildIndex(keyNodes,key,keySize);
        if(leafIndex >= keyNodes.size()) return  new ArrayList<byte[]>(4);
        int posStart = 0,posEnd = keyNodes.get(leafIndex).pos;
        if(leafIndex -1 >= 0){
            posStart = keyNodes.get(leafIndex-1).pos;
        }

        List<byte[]> kvs = getValuesByPosRange(posStart, posEnd);
        return  RankUtil.getResultByKey(kvs, key);
    }


    private List<byte[]> _getResultByRange(int index,byte[] from, byte[] to)throws IOException{
        if(index >= keyNodes.size()){
            return new ArrayList<byte[]>(4);
        }
        int posStart = 0,posEnd = keyNodes.get(index).pos;
        if(index -1 >= 0){
            posStart = keyNodes.get(index-1).pos;
        }
        return _getResultByRange(posStart, posEnd, from, to);
    }
    private List<byte[]> _getResultByRange(int posStart, int posEnd, byte[] from, byte[] to)throws IOException{

        List<byte[]> kvs = getValuesByPosRange(posStart, posEnd);
        return  RankUtil.getResultByRange(kvs, from, to);
    }

    public List<byte[]> query(byte[] from, byte[] to)throws IOException{
        checkReRank();
        if(from.length != keySize || to.length != keySize){
            throw new RuntimeException(String.format("%d != %d or %d != %d",from.length, keySize, to.length, keySize));
        }
        //TODO 根据范围查询
        int leafStart = findChildIndex(keyNodes, from, from.length);
        int leafEnd = findChildIndex(keyNodes, to, to.length);
        if(leafStart >= keyNodes.size()){
            return new ArrayList<byte[]>(4);
        }
        if(leafStart == leafEnd){
            return _getResultByRange(leafStart, from, to);
        }else {
            List<byte[]> result = new ArrayList<byte[]>(leafStart-leafEnd > 1 ? 2048 : 1024);
            result.addAll(_getResultByRange(leafStart,from, null));
            if(leafEnd - leafStart > 1){
                int posStart = keyNodes.get(leafStart).pos;
                int posEnd = keyNodes.get(leafEnd-1).pos;
                List<byte[]> kvs = getValuesByPosRange(posStart, posEnd);
                result.addAll(kvs);
            }
            result.addAll(_getResultByRange(leafEnd,null, to));
            return result;
        }
    }
    public void checkReRank()throws IOException{
        if(keyNodes.size() > 0) return;
        synchronized (keyNodes){
            if(keyNodes.size() > 0) return;
            StoreConfig.STREE_CHECK_SEMAPHORE.acquireUninterruptibly(1);
            //reRank();
            reRankByTree();
            StoreConfig.STREE_CHECK_SEMAPHORE.release(1);

        }
    }

    private List<byte[]>  deserialize(ByteBuffer buff,int readNum){
        List<byte[]>  kvs = new ArrayList<byte[]>(1024);
        for (int i = 0;i< readNum/(dataSize+keySize);i++) {
            byte[] tmp = new byte[dataSize+keySize];
            buff.get(tmp);
            kvs.add(tmp);
        }
        return kvs;
    }
    public void reRank()throws IOException{
        int readKvNum = 0;
        List<byte[]>  kvs = new ArrayList<byte[]>(kvNum);
        fileChannel.position(0);
        ByteBuffer tmpBuffer = ByteBuffer.allocate(1024 * (dataSize + keySize));
        Loop:
        for (;;){
            int readNum = fileChannel.read(tmpBuffer);
            tmpBuffer.flip();
            for (int i = 0;i< readNum/(dataSize+keySize);i++) {
                byte[] tmp = new byte[dataSize+keySize];
                tmpBuffer.get(tmp);
                kvs.add(tmp);
                if(kvs.size() == kvNum) break Loop;
            }
            tmpBuffer.clear();
            readKvNum += readNum/(dataSize+keySize);
            if(readKvNum >= kvNum){
                break;
            }
        }
        Collections.sort(kvs, new Comparator<byte[]>() {
            @Override
            public int compare(byte[] o1, byte[] o2) {
                return RankUtil.compare(o1,o2,keySize);
            }
        });
        for (int i = 1023; i < kvs.size(); i+=1024){
            int j = i;
            while (true){
                j++;
                if(j >= kvs.size()){
                    break;
                }
                if(RankUtil.compare(kvs.get(j),kvs.get(i),keySize) != 0){
                    break;
                }
            }
            j--;
            keyNodes.add(new Node(kvs.get(j),(j+1)*(dataSize+keySize)));

        }
        //最后一个节点校验
        if(keyNodes.size() == 0 || RankUtil.compare(kvs.get(kvs.size()-1), keyNodes.get(keyNodes.size()-1).key,keySize) != 0){
            keyNodes.add(new Node(kvs.get(kvs.size()-1),(kvs.size())*(dataSize+keySize)));
        }
        fileChannel.position(0);
        ByteBuffer flushBuff = ByteBuffer.allocate(kvs.size() * (keySize + dataSize));

        PrintUtil.print("rerank size:%d kvNum:%d keySize:%d dataSize:%d", kvs.size(),kvNum, keySize, dataSize);
        for (int i = 0; i < kvs.size(); i++) {
            //System.out.println(TypeUtil.bytesToLong(Arrays.copyOfRange(kvs.get(i),0,4)));
            flushBuff.put(kvs.get(i));

        }
        flushBuff.flip();
        fileChannel.write(flushBuff);
        kvs.clear();
    }




    public void reRankByTree()throws IOException{
        int readKvNum = 0;
        TreeSet<byte[]>  kvs = new TreeSet<byte[]>(new Comparator<byte[]>() {
            @Override
            public int compare(byte[] o1, byte[] o2) {
                return RankUtil.compare(o1,o2, keySize);
            }
        });
        fileChannel.position(0);
        ByteBuffer tmpBuffer = ByteBuffer.allocate(1024 * (dataSize + keySize));
        Loop:
        for (;;){
            int readNum = fileChannel.read(tmpBuffer);
            tmpBuffer.flip();
            for (int i = 0;i< readNum/(dataSize+keySize);i++) {
                byte[] tmp = new byte[dataSize+keySize];
                tmpBuffer.get(tmp);
                kvs.add(tmp);
                if(kvs.size() == kvNum) break Loop;
            }
            tmpBuffer.clear();
            readKvNum += readNum/(dataSize+keySize);
            if(readKvNum >= kvNum){
                break;
            }
        }

        fileChannel.position(0);
        ByteBuffer flushBuff = ByteBuffer.allocateDirect(512 * 1024 * (keySize + dataSize));
        int i=0;
        Iterator<byte[]> kvIt = kvs.iterator();
        byte[] last = null;
        while (kvIt.hasNext()){
            last = kvIt.next();
            if(++i % 1024 == 0){
                keyNodes.add(new Node(last, i * (dataSize+keySize)));
            }
            if(flushBuff.remaining() < last.length){
                flushBuff.flip();
                fileChannel.write(flushBuff);
                flushBuff.clear();
            }
            flushBuff.put(last);
        }
        //最后一个节点校验
        if(keyNodes.size() == 0 || RankUtil.compare(last, keyNodes.get(keyNodes.size()-1).key,keySize) != 0){
            keyNodes.add(new Node(last,(kvs.size())*(dataSize+keySize)));
        }
        PrintUtil.print("rerank size:%d kvNum:%d keySize:%d dataSize:%d", kvs.size(),kvNum, keySize, dataSize);
        flushBuff.flip();
        fileChannel.write(flushBuff);
        flushBuff.clear();
        kvs.clear();
    }


    public int findChildIndex(List<Node> nodes, byte[] key, int keySize){
        int start = 0,end = nodes.size()-1;
        while (true){
            try {
                if(start == end){
                    int res = RankUtil.compare(key,nodes.get(start).key,keySize);
                    return  res <= 0 ? start : start + 1;
                }
                int mid = (start+end)/2;
                int res = RankUtil.compare(key, nodes.get(mid).key, keySize);
                if(res == 0) return mid;

                if(res < 0){
                    end = mid; //注意不是mid -1
                }else {
                    start = mid + 1; //注意是mid+1
                }
            }catch (Exception e){
                PrintUtil.print("start:%d end:%d size:%d", start, end, nodes.size());
                e.printStackTrace();
                System.exit(1);
            }

        }
    }
}
