package simpledb;

import java.io.*;
import java.util.*;
import java.io.FileInputStream;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private File f;
    private TupleDesc td;
    private static Map<Integer, Integer> file2id = new HashMap<>(); //file to uid
    private static int uid=0;
    private int countPgNum;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
        countPgNum = (int)Math.ceil(1.0*f.length()/BufferPool.getPageSize());
        //file2id = new HashMap<>();

        if(file2id.containsKey(f.getAbsoluteFile().hashCode()) ==false){
            file2id.put(f.getAbsoluteFile().hashCode(), uid++);
        }
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */


    public int getId() {
        // some code goes here
        //System.out.println(f.getAbsoluteFile());

        return file2id.get(f.getAbsoluteFile().hashCode());
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        Page pg = null;
        try{
            RandomAccessFile raf = new RandomAccessFile(f,"r");
            int pgSize = BufferPool.getPageSize();
            int offset = pid.pageNumber()*pgSize;
            raf.seek(offset);
            byte[] bytes = new byte[(int)pgSize];
            raf.read(bytes);
            HeapPageId pgId = new HeapPageId(pid.getTableId(),pid.pageNumber());
            pg = new HeapPage(pgId,bytes);
            //System.out.println(((HeapPage) pg).getNumEmptySlots());

        }catch(IOException e){

            e.printStackTrace();
        }



        return pg;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        raf.seek(page.getId().pageNumber()*BufferPool.getPageSize());
        raf.write(page.getPageData(),0,page.getPageData().length);

    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return countPgNum;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        HeapPage page;
        ArrayList<Page> result = new ArrayList<>();

        //if(numPages() == 0) System.out.println("Yes!");

        for(int i = 0;i<numPages();i++){
            //System.out.println("Here "+getId());
            page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            if(page.getNumEmptySlots() > 0){
                //System.out.println("Here");
                //System.out.println(i+ " "+page.getNumEmptySlots());
                page.insertTuple(t);
                result.add(page);
                //writePage(page);
                Database.getBufferPool().releasePage(tid, page.getId());
                //Database.getLockManager().Unlock(page.getId(), tid);
                break;
            }else{
                Database.getBufferPool().releasePage(tid, page.getId());
                //Database.getLockManager().Unlock(page.getId(), tid);
            }
        }

        if(result.size() == 0){
            //System.out.println("Here????");
            //System.out.println(numPages());

            byte[]data = HeapPage.createEmptyPageData();
            page = new HeapPage(new HeapPageId(getId(), numPages()),data);
            t.setRecordId(new RecordId(new HeapPageId(getId(), numPages()),0));
            page.insertTuple(t);
            //writePage(page);
            //System.out.println(page.getNumEmptySlots()+" "+numPages());
            result.add(page);
            //page.markDirty(true, tid);
            countPgNum++;
        }




        return result;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        //return null;
        // not necessary for lab1
        HeapPage page = null;
        ArrayList<Page> ret = new ArrayList<>();
        for(int i = 0;i<numPages();i++){
            //System.out.println("Here????");
            page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(),i), Permissions.READ_WRITE);
            try{
                page.deleteTuple(t);
                ret.add(page);
                //writePage(page);
            }catch(DbException  e){
                //Database.getBufferPool().releasePage(tid, page.getId());
            }
            //Database.getBufferPool().
            //Database.getLockManager().Unlock(page.getId(), tid);
            Database.getBufferPool().releasePage(tid, page.getId());

        }
        if(ret.size() == 0){
            throw new DbException("Not in file");
        }
        return ret;
    }

    class myIter implements DbFileIterator{
        public int pid;
        public TransactionId tid;
        public int tableId;
        public int pgNumber;
        public int maxPgNum;
        public Page curPg;
        public Iterator<Tuple> iter;
        public boolean isClosed;
        public myIter(TransactionId tid, int tableId){
            this.tid = tid;
            this.tableId= tableId;
            this.pgNumber = 0;
            maxPgNum = numPages();
            isClosed = true;
            iter = null;
            //System.out.println("Here in HeapFile");
            //curPg = Database.getBufferPool().getPage(tid,
            //        new HeapPageId(tableId, pgNumber), Permissions.READ_ONLY);
            //System.out.println(maxPgNum);


        }
        public void open()throws TransactionAbortedException, DbException{
            isClosed = false;
            System.out.println("Open the file!!!");
            curPg = Database.getBufferPool().getPage(tid,
                    new HeapPageId(tableId, pgNumber), Permissions.READ_ONLY);
            //System.out.println("Here "+curPg.hashCode());
            iter = curPg.iterator();
        }
        public boolean hasNext() throws TransactionAbortedException, DbException{

            if(isClosed) return false;
            while( (iter == null || iter.hasNext()==false) && pgNumber<maxPgNum-1){
                pgNumber++;

                Database.getBufferPool().releasePage(tid, curPg.getId());

                open();
            }
            if(iter == null) return false;
            return iter.hasNext();
        }
        public Tuple next()
                throws DbException, TransactionAbortedException, NoSuchElementException{
            if(isClosed) throw new NoSuchElementException();
            return iter.next();
        }
        public void rewind() throws DbException, TransactionAbortedException{
            if(isClosed) throw new DbException("Closed!");
            pgNumber = 0;

            Database.getBufferPool().releasePage(tid, curPg.getId());
            //Database.getLockManager().Unlock(curPg.getId(), tid);
            open();
        }
        public void close(){
            isClosed = true;

            Database.getBufferPool().releasePage(tid, curPg.getId());
            //Database.getLockManager().Unlock(curPg.getId(), tid);
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new myIter(tid, getId());
    }

}

