package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private DbIterator dbiter;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    private DbIterator child;
    private int afield;
    private int gfield;
    private Type gfieldtype;
    private Aggregator.Op aop;
    private Type afieldtype;
    private TupleDesc tdes;


    private DbIterator[] children;

    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        afieldtype = child.getTupleDesc().getFieldType(afield);
        gfieldtype = gfield == -1 ? null : child.getTupleDesc().getFieldType(gfield);
        children = null;


        Type typeArr[] = null;
        String fieldArr[] = null;
        if(gfield == -1){
            typeArr = new Type[]{gfieldtype};
            fieldArr = new String[]{"GroupBy"};
        }else{
            typeArr = new Type[]{gfieldtype, afieldtype};
            fieldArr = new String[]{"GroupBy", "aggName("+aop+")"+
                    " ("+child.getTupleDesc().getFieldName(afield)};
        }

        tdes = new TupleDesc(typeArr, fieldArr);
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
        if(gfield == -1){
            return Aggregator.NO_GROUPING;
        }else{
            return gfield;
        }
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
	//return null;
        if(gfield == -1) return null;
        return child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	    return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
	//return null;
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	//return null;
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	    return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	    // some code goes here
        super.open();
        child.open();
        Aggregator agg;
        if(gfield==-1){
            //System.out.println("No Group");
            agg=new NoGroupAggregator(gfield,gfieldtype);
        }else if(afieldtype == Type.INT_TYPE){
            //System.out.println("Int Type");
            //System.out.println(aop);
            agg = new IntegerAggregator(gfield,gfieldtype,afield,aop);
        }else{
            //System.out.println("String Type");
            agg = new StringAggregator(gfield,gfieldtype, afield,aop);
        }

        child.rewind();
        while(child.hasNext()){
            agg.mergeTupleIntoGroup(child.next());
        }

        dbiter = agg.iterator();
        dbiter.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
	//return null;
        //System.out.println("in fetchNext");
        if(dbiter.hasNext() == false) return null;
        //System.out.println("After hasNext");
	    return dbiter.next();
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
        dbiter.rewind();
        child.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
	//eturn null;


        return tdes;
    }

    public void close() {
	// some code goes here
        super.close();
        dbiter.close();
        child.close();
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
