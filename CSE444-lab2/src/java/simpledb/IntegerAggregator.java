package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private Map<Field, List<Tuple>> map;

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;
        this.afield = afield;
        map = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        //System.out.println("In Merge: "+tup);
        Field fd = tup.getField(gbfield);
        if(map.containsKey(fd) == false){
            map.put(fd, new ArrayList<>());
        }
        map.get(fd).add(tup);
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    class MyIter implements DbIterator{

        private Iterator<Map.Entry<Field, List<Tuple>>> iter;
        private boolean isOpen;
        private TupleDesc tdes;
        public MyIter(){

            isOpen = false;
        }
        public void open()
                throws DbException, TransactionAbortedException{
            iter = map.entrySet().iterator();
            isOpen = true;
        }
        public boolean hasNext() throws DbException, TransactionAbortedException{
            if(isOpen == false) throw new IllegalStateException();
            return iter.hasNext();
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException{
            if(hasNext() == false) throw new NoSuchElementException();
            if(isOpen == false) throw new IllegalStateException();

            Map.Entry<Field, List<Tuple>> pair = iter.next();
            int result = 0;
            if(what == Op.MIN){
                result = Integer.MAX_VALUE;
            }else if(what == Op.MAX){
                result = Integer.MIN_VALUE;
            }else{
                result = 0;
            }

            for(Tuple tp : pair.getValue()){
                //System.out.println("In for loop: "+tp+" "+gbfield+" "+afield);
                switch(what){
                    case MIN:
                        result = Math.min(result, ((IntField)tp.getField(afield)).getValue());
                        break;
                    case MAX:
                        result = Math.max(result, ((IntField)tp.getField(afield)).getValue());
                        break;
                    default:
                        result += ((IntField)tp.getField(afield)).getValue();
                        //System.out.println(result);
                        break;
                }
            }

            TupleDesc td = null;
            Type typeArr [] = new Type[2];
            String fieldAr[] = new String[2];
            typeArr[0] = gbfieldtype;
            typeArr[1] = Type.INT_TYPE;
            fieldAr[0] = "GroupBy";
            fieldAr[1] = "Aggregate";
            if(what == Op.AVG) result /= pair.getValue().size();
            else if(what == Op.COUNT) result = pair.getValue().size();

            td = new TupleDesc(typeArr, fieldAr);
            Tuple ret = new Tuple(td);
            ret.setField(0, pair.getKey());
            ret.setField(1, new IntField(result));

            return ret;
        }

        public void rewind() throws DbException, TransactionAbortedException{
            if(isOpen == false) throw new IllegalStateException();
            open();
        }

        public TupleDesc getTupleDesc(){
            return tdes;
        }

        public void close(){
            isOpen = false;
        }

    }

    public DbIterator iterator() {
        // some code goes here
        return new MyIter();
    }

}
