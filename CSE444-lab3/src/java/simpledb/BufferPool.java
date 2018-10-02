package simpledb;

import java.io.*;
import java.util.*;

import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;
    private int numPages;
    private static int pageSize = PAGE_SIZE;
    private Map<TransactionId, List<PageId>> tid2pid;

    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final LockManager lm;

    private Map<PageId, Page> pools;
    private Queue<PageId> queue;

    public LockManager getLM(){
        return lm;
    }


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        pools = new HashMap<>();
        queue=new LinkedList<>();
        lm = new LockManager();
        tid2pid = new HashMap<>();
        //System.out.println("Here");
        //System.out.println(lm.getHold().size());
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        //Page page = DbFile.readPage(pid);
        //System.out.println("Gere");
        //System.out.println(perm);
        LockManager.LockType lockType = perm == Permissions.READ_ONLY?
                LockManager.LockType.Shared: LockManager.LockType.Mutex;

        lm.Lock(pid, lockType, tid);
        if(tid2pid.containsKey(tid) == false){
            tid2pid.put(tid, new ArrayList<>());
        }
        tid2pid.get(tid).add(pid);
        if(pools.containsKey(pid)){

            return pools.get(pid);
        }

        if(pools.size()>=numPages){
            evictPage();
            //throw new DbException("No enough space");
        }

        //System.out.println(pid.getTableId());
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page pg = dbFile.readPage(pid);
        pools.put(pid, pg);
        queue.offer(pid);
        //LockManager.Unlock(pid, perm);
        //Database.getBufferPool().getLM().getHoldCount(pid, tid);
        //tid2pid.get(tid).add(pg);
        return pg;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lm.Unlock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lm.HoldLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        if(commit == true){
            flushPages(tid);
        }else{
            List<PageId> pids = tid2pid.get(tid);
            tid2pid.remove(tid);
            for(PageId pid : pids){
                pools.remove(pid);
                try{
                    Database.getBufferPool().getPage(tid,pid,Permissions.READ_ONLY);
                }catch(TransactionAbortedException|DbException e){
                    e.printStackTrace();
                }
                Database.getBufferPool().releasePage(tid, pid);

            }
            tid2pid.remove(tid);
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile df = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> ret =  df.insertTuple(tid, t);
        for(Page pg : ret){
            //Database.getBufferPool().releasePage(tid, pg.getId());
            pg.markDirty(true, tid);
            //if(!pools.containsKey(pg.getId())){
                queue.offer(pg.getId());
            //}
            pools.put(pg.getId(), pg);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        DbFile df = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> ret = df.deleteTuple(tid,t);
        for(Page pg : ret){
            //Database.getBufferPool().releasePage(tid, pg.getId());
            pg.markDirty(true, tid);
            pools.put(pg.getId(),pg);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for(PageId pid : pools.keySet()){
            flushPage(pid);
            //System.out.println("Here");
            //lm.UnlockPage(pid);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab5
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        if(pools.containsKey(pid) == false) return;
        Page pg = pools.get(pid);
        //System.out.println(pg);
        if(pg.isDirty() == null) return;
        pg.markDirty(false,new TransactionId());
        DbFile df = Database.getCatalog().getDatabaseFile(pid.getTableId());
        df.writePage(pg);

    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        if(tid2pid.containsKey(tid) == false) return;
        for(PageId pid : tid2pid.get(tid)){
            flushPage(pid);
            Database.getBufferPool().releasePage(tid, pid);
        }
        tid2pid.remove(tid);
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        boolean isSuc = false;
        while(queue.isEmpty() == false){
            PageId pid = queue.poll();
            if(pools.containsKey(pid) && pools.get(pid).isDirty() == null && lm.IsLock(pid) == false){
                try{
                    flushPage(pid);
                    pools.remove(pid);
                    isSuc = true;
                    break;
                }catch(IOException e){
                    e.printStackTrace();
                }
            }
        }
        if(isSuc == false){
            throw new DbException("All dirty!");
        }



    }

}
