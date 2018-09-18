package simpledb;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public class AggregatorIteratorNoGroup implements DbIterator {
    private Iterator<Tuple> iter;
    private boolean isOpen;
    private TupleDesc tdes;

    private Set<Tuple> set;
    private int gbfield;
    private Type gbfieldtype;


    public AggregatorIteratorNoGroup(Set<Tuple> set,
                                     int gbfield, Type gbfieldtype){

        this.set = set;
        isOpen = false;
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;

        //create tupledesc
        Type typeArr[] = new Type[]{gbfieldtype};
        String fieldArr[] = new String[]{"GfroupBy"};
        tdes = new TupleDesc(typeArr, fieldArr);


    }
    public void open()
            throws DbException, TransactionAbortedException{
        iter = set.iterator();
        isOpen = true;
    }
    public boolean hasNext() throws DbException, TransactionAbortedException{
        if(isOpen == false) throw new IllegalStateException();
        return iter.hasNext();
    }

    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if(hasNext() == false) throw new NoSuchElementException();
        if(isOpen == false) throw new IllegalStateException();

        return iter.next();
    }

    public void rewind() throws DbException, TransactionAbortedException{
        if(isOpen == false) throw new IllegalStateException();
        open();
    }

    public TupleDesc getTupleDesc(){
        //System.out.println("Here???");
        return tdes;
    }

    public void close(){
        isOpen = false;
    }
}
