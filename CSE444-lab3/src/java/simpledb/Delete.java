package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    private TransactionId tid;
    private DbIterator child;
    private DbIterator[] children;
    private boolean firstTime;
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        this.tid = t;
        this.child = child;
        children = null;
        firstTime = true;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"Count"});
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        //System.out.println("Come here");
        if(firstTime == false) return null;
        firstTime = false;
        int count = 0;
        //System.out.println("Here????");
        while(child.hasNext()){
            //System.out.println("Here");
            Tuple tp = child.next();
            try{
                Database.getBufferPool().deleteTuple(tid,tp);
                count++;
            }catch(IOException e){

            }
        }
        //System.out.println(count);
        TupleDesc td = getTupleDesc();
        Tuple ret = new Tuple(td);
        ret.setField(0,new IntField(count));
        return ret;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.children = children;
    }

}
