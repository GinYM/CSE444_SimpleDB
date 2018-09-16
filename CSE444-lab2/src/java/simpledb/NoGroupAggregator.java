package simpledb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class NoGroupAggregator implements Aggregator {

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


    private Set<Tuple> set;

    public NoGroupAggregator(int gbfield, Type gbfieldtype) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        set = new HashSet<>();
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

        set.add(tup);

    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */


    public DbIterator iterator() {
        // some code goes here
        return new AggregatorIteratorNoGroup(set,gbfield,gbfieldtype);
    }

}
