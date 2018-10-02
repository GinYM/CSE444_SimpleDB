package simpledb;

import java.util.*;

public class LockManager {
    // each page assigns one lock
    public Map<PageId, PageLock> map;
    public Map<TransactionId, Set<TransactionId>> graph;

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



    public void Lock(PageId pid, LockType lockType, TransactionId tid)throws TransactionAbortedException{
        if(isDeadLock(tid,pid, lockType)){
            throw new TransactionAbortedException();
        }

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
            for(Request req : pl.requestQueue){
                if(graph.containsKey(req.tid) == false){
                    graph.put(req.tid, new HashSet<>());
                }
                graph.get(req.tid).add(tid);
            }
        }else {
            Request rq = new Request(lockType, tid);
            pl.requestQueue.addLast(rq);
            if(graph.containsKey(tid) == false){
                graph.put(tid, new HashSet<>());
                for(TransactionId tranId : pl.lockOwners){
                    graph.get(tid).add(tranId);
                }
            }
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
        for(Request req : pl.requestQueue){
            graph.get(req.tid).remove(tid);
        }
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

                for(TransactionId father : pl.lockOwners){
                    for(Request child : pl.requestQueue){
                        graph.get(child.tid).add(father);
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

    public boolean IsLock(PageId pid){
        if(map.containsKey(pid) == false || map.get(pid).lockType == LockType.Empty){
            return false;
        }
        return true;
    }

    public boolean isDeadLock(TransactionId tid, PageId pid, LockType lockType){
        if(map.containsKey(pid) == false || map.get(pid).lockOwners.size() == 0){
            return false;
        }
        PageLock pl = map.get(pid);
        if(pl.lockType == LockType.Shared && lockType == pl.lockType){
            return false;
        }
        if(pl.lockOwners.size() == 1 && pl.lockOwners.contains(tid)){
            return false;
        }
        for(TransactionId sourceid : pl.lockOwners){
            if(sourceid.equals(tid)){
                continue;
            }
            if(isContain(sourceid, tid)){
                return true;
            }
        }
        return false;


    }

    private boolean isContain(TransactionId tid, TransactionId target){
        if(tid.equals(target)){
            return true;
        }

        for(TransactionId father : graph.get(tid)){
            if(isContain(father, target)){
                return true;
            }
        }
        return false;
    }

}
