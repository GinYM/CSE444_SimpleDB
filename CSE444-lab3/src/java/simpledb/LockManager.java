package simpledb;

import java.util.*;

public class LockManager {
    // each page assigns one lock
    public static Map<PageId, PageLock> map = new HashMap<>();
    public static Map<TransactionId, Set<TransactionId>> graph = new HashMap<>();

    // for synchronize
    private static Object mutex = new Object();

    public LockManager(){
        //mutex = new Object();
        //map = new HashMap<>();
        //graph = new HashMap<>();
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
        private boolean isAcquired;
        public Request(LockType lockType, TransactionId tid){
            this.lockType = lockType;
            this.tid = tid;
            isAcquired = false;
        }
    }

    public boolean isCompatible(PageId pid, LockType lockType, TransactionId tid){
        if(map.containsKey(pid) == false){
            return true;
        }

        PageLock pl = map.get(pid);

        if(pl.lockOwners.size() == 1 &&
                pl.lockOwners.contains(tid) ){
            return true;
        }else if(pl.lockType == LockType.Empty){
            return true;

        }else if(pl.lockType == LockType.Shared && lockType == pl.lockType){
            return true;
        }else {
            return false;

        }

    }



    public void Lock(PageId pid, LockType lockType, TransactionId tid)throws TransactionAbortedException{

        //System.out.println("tid: "+tid.getId()+" owns "+lockType+" "+pid.toString());

        //update graph as long as we detect it has no dead lock
        synchronized (mutex) {
            //System.out.println("Check deadlock "+tid.getId());
            if (isDeadLock(tid, pid, lockType)) {
                throw new TransactionAbortedException();
            }else{
                if(isCompatible(pid, lockType, tid) && map.containsKey(pid)){
                    PageLock pl = map.get(pid);

                    for(Request req : pl.requestQueue){
                        graph.get(req.tid).add(tid);
                    }
                }else if(map.containsKey(pid)){
                    PageLock pl = map.get(pid);
                    if(graph.containsKey(tid) == false){
                        graph.put(tid, new HashSet<>());
                    }
                    //System.out.println(pl.lockOwners.size());
                    for(TransactionId tranid : pl.lockOwners){
                        if(tranid.equals(tid)){
                            continue;
                        }
                        graph.get(tid).add(tranid);
                    }
                }
            }
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
            /*
            for(Request req : pl.requestQueue){
                if(graph.containsKey(req.tid) == false){
                    graph.put(req.tid, new HashSet<>());
                }
                graph.get(req.tid).add(tid);
            }
            */
        }else {
            Request rq = new Request(lockType, tid);
            pl.requestQueue.addLast(rq);


            try{
                synchronized (rq){
                    while(rq.isAcquired == false){
                        rq.wait(1);
                    }
                    //System.out.println("Wait: "+rq+" "+rq.tid.getId());
                    //rq.wait();
                    //System.out.println("Wake up: "+rq.tid.getId());
                }
            }catch(InterruptedException e){
                e.printStackTrace();
            }

        }
    }

    public void UnlockAllPid(PageId pid, TransactionId tid){
        //System.out.println("Unlock "+tid.getId()+" "+pid.toString());
        synchronized (mutex){
            if(map.containsKey(pid) == false){
                throw new IllegalStateException();
            }
            PageLock pl = map.get(pid);
            pl.lockOwners.remove(tid);
            for(Request req : pl.requestQueue){
                graph.get(req.tid).remove(tid);
            }

            Iterator<Request> iter1 = pl.requestQueue.iterator();
            while(iter1.hasNext()){
                Request next = iter1.next();
                if(next.tid.equals(tid)){
                    iter1.remove();
                }
            }

            if(pl.lockOwners.size() == 0){
                if(pl.requestQueue.size() == 0){
                    pl.lockType = LockType.Empty;
                }else{
                    Request req = pl.requestQueue.pollFirst();

                    pl.lockType = req.lockType;
                    pl.lockOwners.add(req.tid);
                    System.out.println("notify: "+req+" "+req.tid.getId());
                    //req.notify();
                    req.isAcquired = true;
                    Iterator<Request> iter = pl.requestQueue.iterator();
                    if(pl.lockType == LockType.Shared){
                        while(iter.hasNext()){
                            Request next = iter.next();
                            if(next.lockType == LockType.Shared){
                                pl.lockOwners.add(next.tid);
                                iter.remove();
                                next.isAcquired = true;
                            }
                        }
                    }

                    for(TransactionId father : pl.lockOwners){
                        for(Request child : pl.requestQueue){
                            graph.get(child.tid).add(father);
                        }
                    }

                }
            }else if(pl.lockOwners.size() == 1 && pl.lockType == LockType.Shared){
                TransactionId ownTid = pl.lockOwners.iterator().next();
                Iterator<Request> iter = pl.requestQueue.iterator();
                while(iter.hasNext()){
                    Request req = iter.next();
                    if(req.tid.equals(ownTid) && req.lockType == LockType.Mutex){
                        pl.lockType = LockType.Mutex;
                        req.isAcquired = true;
                        iter.remove();
                        break;
                    }
                }
            }
        }




    }



    public void UnlockPage(PageId pid){
        //System.out.println("Here!!!");
        if(map.containsKey(pid) == false){
            //System.out.println("Not contains!!!!");
            return;
        }
        PageLock pl = map.get(pid);
        for(TransactionId tid : pl.lockOwners){
            this.Unlock(pid, tid);
        }
    }

    public void Unlock(PageId pid, TransactionId tid){
        //System.out.println("Unlock "+tid.getId()+" "+pid.toString());
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
                req.isAcquired = true;
                Iterator<Request> iter = pl.requestQueue.iterator();
                if(pl.lockType == LockType.Shared){
                    while(iter.hasNext()){
                        Request next = iter.next();
                        if(next.lockType == LockType.Shared){
                            pl.lockOwners.add(next.tid);
                            iter.remove();
                            next.isAcquired = true;
                        }
                    }
                }

                for(TransactionId father : pl.lockOwners){
                    for(Request child : pl.requestQueue){
                        graph.get(child.tid).add(father);
                    }
                }

            }
        }else if(pl.lockOwners.size() == 1 && pl.lockType == LockType.Shared){
            TransactionId ownTid = pl.lockOwners.iterator().next();
            Iterator<Request> iter = pl.requestQueue.iterator();
            while(iter.hasNext()){
                Request req = iter.next();
                if(req.tid.equals(ownTid) && req.lockType == LockType.Mutex){
                    pl.lockType = LockType.Mutex;
                    req.isAcquired = true;
                    iter.remove();
                    break;
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
        //try{

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

                    //System.out.println("sourceId: "+ sourceid.getId()+" "+tid.getId()+" "+lockType);
                    if(isContain(sourceid, tid)){
                        System.out.println("Dead Lock!!");
                        return true;
                    }

                }

        //}catch (InterruptedException e){
        //    e.printStackTrace();
        //}

        return false;


    }

    private boolean isContain(TransactionId tid, TransactionId target){
        //System.out.println("Is Contain!");
        if(tid.equals(target)){
            return true;
        }

        if(graph.containsKey(tid) == false){
            return false;
        }

        for(TransactionId father : graph.get(tid)){
            //System.out.println(tid.getId()+" father: "+father.getId());
            if(isContain(father, target)){
                return true;
            }
        }
        return false;
    }

}
