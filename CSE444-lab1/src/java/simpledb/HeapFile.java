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
    private static Map<Integer, Integer> file2id; //file to uid
    private static int uid=0;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
        file2id = new HashMap<>();

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
            InputStream is = new FileInputStream(f);
            int pgSize = BufferPool.getPageSize();
            byte[] bytes = new byte[(int)pgSize];
            int offset = pid.pageNumber()*pgSize;
            //System.out.println(pid.pageNumber());
            is.read(bytes, offset, pgSize);
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
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int len = (int)f.length();
        return (int)Math.ceil(1.0*len/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
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
            //curPg = Database.getBufferPool().getPage(tid,
            //        new HeapPageId(tableId, pgNumber), Permissions.READ_ONLY);
            //System.out.println(maxPgNum);


        }
        public void open()throws TransactionAbortedException, DbException{
            isClosed = false;
            curPg = Database.getBufferPool().getPage(tid,
                    new HeapPageId(tableId, pgNumber), Permissions.READ_ONLY);

            iter = curPg.iterator();
        }
        public boolean hasNext() throws TransactionAbortedException, DbException{

            if(isClosed) return false;
            while( (iter == null || iter.hasNext()==false) && pgNumber<maxPgNum-1){
                pgNumber++;
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
            open();
        }
        public void close(){
            isClosed = true;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new myIter(tid, getId());
    }

}

