package btree;

import com.alibaba.middleware.race.store.Page;
import com.alibaba.middleware.race.store.PageMgr;
import com.alibaba.middleware.race.store.StoreConfig;
import org.junit.Test;

/**
 * Created by liuzhendong on 16/7/19.
 */
public class PageTest {

    @Test
    public void testPage()throws Exception{
        PageMgr pageMgr = PageMgr.createNew("/tmp/tmp_page.txt",1024*1024);
        Page page = pageMgr.allocateNewPage();
        page.flushIntoDisk("this is page one".getBytes());
    }

    @Test
    public void testLoad()throws Exception{
        PageMgr pageMgr = PageMgr.load("/tmp/tmp_page.txt");
        Page page = pageMgr.getPageByPos(pageMgr.firstAllocatedPos);
        System.out.println(new String(page.readFromDisk()));
        Page second = pageMgr.getPageByPos(pageMgr.firstAllocatedPos+ StoreConfig.DEFAULT_PAGE_SIZE);
        page.flushIntoDisk("this is page two".getBytes());
        System.out.println(new String(second.readFromDisk()));
    }
}
