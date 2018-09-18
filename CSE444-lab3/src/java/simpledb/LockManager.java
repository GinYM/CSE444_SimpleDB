package simpledb;

import java.util.*;
import java.util.concurrent.Semaphore;

public class LockManager {
    // each page assigns one lock
    public static final int lockSize = 10000;
    public static Map<PageId, SemaphoreLock> map = new HashMap<>();
    // Page hold by transactionId
    public static Map<TranPage, MyLock> hold = new HashMap<>();

    //private Map<PageId, Integer> count = new HashMap<>();

    public Map<TranPage, MyLock> getHold(){
        return hold;
    }

    public void getHoldCount(PageId pid, TransactionId tid){
        TranPage tp = new TranPage(pid, tid);
        MyLock ml = hold.get(tp);
        //System.out.println("Count "+ml.rwlock.getReadHoldCount()+" perm "+ml.perm);
    }

    public static enum LockType{
        Shared, Mutex, Empty
    }

    class SemaphoreLock {
        public Semaphore shared;
        public Semaphore mutex;
        private Object obj;
        private Object mutexObj;
        private int count;
        private LockType type;

        // lock the critical section
        private Semaphore critical;

        public SemaphoreLock(){
            shared = new Semaphore(lockSize);
            mutex = new Semaphore(1);
            obj = new Object();
            count = 0; //count the thread acquiring lock
            type = LockType.Empty;
            critical = new Semaphore(1);
            mutexObj = new Object();
        }
        public void acquireShare(){
            try{
                critical.acquire();

                if(type == LockType.Mutex){
                    critical.release();
                    synchronized (mutexObj){
                        mutexObj.wait();
                    }
                }else{
                    type = LockType.Shared;
                    critical.release();
                }

                //critical.release();

                shared.acquire();
                critical.acquire();
                count++;
                critical.release();
                //count++;
                //System.out.println("Acquire shared lock"+shared.hasQueuedThreads());

            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
        public void releaseShare(){

            try{
                critical.acquire();
                count--;
                critical.release();
            }catch(InterruptedException e){
                e.printStackTrace();
            }

            shared.release();

            try{
                critical.acquire();
                if(count == 0){
                    critical.release();
                    synchronized (obj){
                        obj.notifyAll();
                    }
                    //this.notifyAll();
                }else{
                    critical.release();
                }
            }catch(InterruptedException e){
                e.printStackTrace();
            }

            //count--;

        }

        public void acquireMutex(){
            try{
                critical.acquire();

                if(count != 0 ){
                    critical.release();
                    synchronized (obj){
                        obj.wait();
                    }
                }else{
                    type = LockType.Mutex;
                    critical.release();
                }


                mutex.acquire();
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }

        public void releaseMutex(){
            if(mutex.hasQueuedThreads() == false){
                synchronized (mutexObj){
                    mutexObj.notifyAll();
                }
                try{
                    critical.acquire();
                    type=LockType.Empty;
                    critical.release();
                }catch(InterruptedException e){
                    e.printStackTrace();
                }
            }
            mutex.release();
        }

    }

    class MyLock{
        public SemaphoreLock rwlock;
        public Permissions perm;
        public MyLock(SemaphoreLock lock, Permissions perm){
            this.rwlock = lock;
            this.perm = perm;
        }
        public void lock(){
            if(perm.equals(Permissions.READ_ONLY)){
                //System.out.println("Lock1212");
                rwlock.acquireShare();
            }else{
                rwlock.acquireMutex();
            }
        }
        public void unlock(){
            if(perm.equals(Permissions.READ_ONLY)){
                rwlock.releaseShare();
            }else{
                rwlock.releaseMutex();
            }
        }
    }

    class TranPage{
        public PageId pid;
        public TransactionId tid;
        public TranPage(PageId pid, TransactionId tid){
            this.pid = pid;
            this.tid = tid;
        }
        public int hashCode(){
            int n = 31;
            int result = 1;
            result = result*n + pid.hashCode();
            result = result*n + tid.hashCode();
            return result;
        }

        public boolean equals(Object tp){
            if(!(tp instanceof TranPage)) return false;

            if(pid.equals(((TranPage)tp).pid) && tid.equals(((TranPage)tp).tid)){
                return true;
            }else{
                return false;
            }
        }
    }

    public void UnlockPage(PageId pid){
        //System.out.println("Here in unlock");
        Iterator<Map.Entry<TranPage, MyLock>> iter = hold.entrySet().iterator();
        //Iterator<TranPage> iter = hold.keySet().iterator();
        //int count = 0;
        while(iter.hasNext()){
            Map.Entry<TranPage, MyLock> entry = iter.next();
            TranPage tp = entry.getKey();
            if(tp.pid.equals(pid)){
                //System.out.println(count++);
                //System.out.println("In UnlockPage Unlock "+entry.getKey().pid.hashCode()+" "+entry.getValue().perm+tp.tid.hashCode());

                entry.getValue().unlock();
                iter.remove();
            }
        }
    }

    public void Lock(PageId pid, Permissions perm, TransactionId tid){
        //System.out.println("Lock "+pid.hashCode()+" "+perm+" "+tid.hashCode());
        if(map.containsKey(pid) == false){
            map.put(pid, new SemaphoreLock());
        }
        //System.out.println(pid+" "+tid);
        TranPage tp = new TranPage(pid, tid);
        MyLock ml;
        if(hold.containsKey(tp)){
            //System.out.println("Here in new "+perm);
            //System.out.println("herh");
            ml = hold.get(tp);
            //System.out.println("Unlock in if "+ml.perm+" "+pid.hashCode()+tid.hashCode());
            //System.out.println(ml.rwlock.getReadHoldCount());
            //System.out.println("hold size "+hold.size());
            //System.out.println(ml.rwlock.getWriteHoldCount());
            ml.unlock();
            /*if(ml.rwlock.getReadHoldCount() > 0){
                ml.unlock();
            }
            else{
                System.out.println("Weird "+pid.hashCode()+" "+tid.hashCode());
            }
            */
            ml.perm = perm;
        }else{
            //System.out.println("New "+pid.hashCode()+" "+tid.hashCode()+" "+perm);
            ml = new MyLock(map.get(pid), perm);
        }
        //System.out.println("Real lock "+pid.hashCode()+" "+perm);
        //System.out.println(ml.rwlock.getWriteHoldCount());
        ml.lock();
        //System.out.println(ml.rwlock.getReadHoldCount());
        //System.out.println("After lock "+pid.hashCode());
        hold.put(tp, ml);
        //System.out.println("Here after hold "+hold.get(new TranPage(pid, tid)).rwlock.getReadHoldCount());
    }
    public void Unlock(PageId pid, TransactionId tid){
        //System.out.println("Unlock "+pid.hashCode()+" "+tid.hashCode());
        TranPage tp = new TranPage(pid, tid);
        if(hold.containsKey(tp) == false){
            System.out.println("Should not be here");
            return;
        }
        //System.out.println("Unlock "+hold.get(tp).rwlock.getWriteHoldCount());
        hold.get(tp).unlock();
        hold.remove(tp);
        //count.put(pid, count.get(pid)-1);
    }

    public boolean HoldLock(TransactionId tid, PageId pid){
        return hold.containsKey(new TranPage(pid, tid));
    }

}
