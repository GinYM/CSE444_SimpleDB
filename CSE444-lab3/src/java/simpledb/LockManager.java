package simpledb;

import java.util.*;

public class LockManager {
    // each page assigns one lock
    public static Map<PageId, PageLock> map = new HashMap<>();

    // for synchronize
    private Object mutex;

    public LockManager(){
        mutex = new Object();
        map = new HashMap<>();
    }


    public static enum LockType{
        Shared, Mutex, Empty
    }



    private class PageLock{
        private LockType lockType;
        private HashSet<TransactionId> lockOwners;
        private LinkedList<Request> requestQueue;
        public PageLock(){
            this.lockType = LockType.Empty;
            lockOwners = new HashSet<>();
            requestQueue = new LinkedList<>();
        }
    }

    private class Request{
        private LockType lockType;
        private TransactionId tid;
        public Request(LockType lockType, TransactionId tid){
            this.lockType = lockType;
            this.tid = tid;
        }
    }



    public void Lock(PageId pid, LockType lockType, TransactionId tid){
        if(map.containsKey(pid) == false){
            map.put(pid, new PageLock());
        }
        PageLock pl = map.get(pid);

        if(pl.lockOwners.size() == 1 &&
                pl.lockOwners.contains(tid) ){
            pl.lockType = lockType;
        }else if(pl.lockType == LockType.Empty){
            pl.lockOwners.add(tid);
            pl.lockType = lockType;

        }else if(pl.lockType == LockType.Shared && lockType == pl.lockType){
            pl.lockOwners.add(tid);
        }else {
            Request rq = new Request(lockType, tid);
            pl.requestQueue.addLast(rq);
            try{
                synchronized (rq){
                    rq.wait();
                }
            }catch(InterruptedException e){
                e.printStackTrace();
            }

        }




    }
    public void Unlock(PageId pid, TransactionId tid){
        //System.out.println("Unlock "+pid.hashCode()+" "+tid.hashCode());
        if(map.containsKey(pid) == false){
            throw new IllegalStateException();
        }
        PageLock pl = map.get(pid);
        pl.lockOwners.remove(tid);
        if(pl.lockOwners.size() == 0){
            if(pl.requestQueue.size() == 0){
                pl.lockType = LockType.Empty;
            }else{
                Request req = pl.requestQueue.pollFirst();

                pl.lockType = req.lockType;
                pl.lockOwners.add(req.tid);
                req.notify();
                Iterator<Request> iter = pl.requestQueue.iterator();
                if(pl.lockType == LockType.Shared){
                    while(iter.hasNext()){
                        Request next = iter.next();
                        if(next.lockType == LockType.Shared){
                            pl.lockOwners.add(next.tid);
                            iter.remove();
                            next.notify();
                        }
                    }
                }

            }
        }

    }

    public boolean HoldLock(TransactionId tid, PageId pid){
        if(map.containsKey(pid) == false ||
                map.get(pid).lockOwners.contains(tid) == false){
            return false;
        }
        return true;
    }

}
