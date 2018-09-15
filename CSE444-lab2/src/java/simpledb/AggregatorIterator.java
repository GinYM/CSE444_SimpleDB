package simpledb;

import java.util.*;

public class AggregatorIterator implements DbIterator {
    private Iterator<Map.Entry<Field, List<Tuple>>> iter;
    private boolean isOpen;
    private TupleDesc tdes;
    private Aggregator.Op what;
    Map<Field, List<Tuple>> map;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Type aggType;

    public AggregatorIterator(Aggregator.Op what, Map<Field, List<Tuple>> map,
                              int gbfield, Type gbfieldtype, int afield, Type aggType){
        this.what = what;
        this.map = map;
        isOpen = false;
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.aggType = aggType;
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

    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if(hasNext() == false) throw new NoSuchElementException();
        if(isOpen == false) throw new IllegalStateException();

        Map.Entry<Field, List<Tuple>> pair = iter.next();
        int result = 0;
        if(what == Aggregator.Op.MIN){
            result = Integer.MAX_VALUE;
        }else if(what == Aggregator.Op.MAX){
            result = Integer.MIN_VALUE;
        }else{
            result = 0;
        }

        String str = "";

        for(Tuple tp : pair.getValue()){
            //System.out.println("In for loop: "+tp+" "+gbfield+" "+afield);
            switch(what){
                case MIN:
                    if(aggType == Type.INT_TYPE){
                        result = Math.min(result, ((IntField)tp.getField(afield)).getValue());
                    }
                    else{
                        String val = ((StringField)tp.getField(afield)).getValue();
                        if(str.length() == 0){
                            str = val;
                        }else{
                            if(str.compareTo(val) > 0){
                                str = val;
                            }
                        }
                    }

                    break;
                case MAX:
                    if(aggType == Type.INT_TYPE){
                        result = Math.max(result, ((IntField)tp.getField(afield)).getValue());
                    }else{
                        String val = ((StringField)tp.getField(afield)).getValue();
                        if(str.length() == 0){
                            str = val;
                        }else{
                            if(str.compareTo(val) < 0){
                                str = val;
                            }
                        }
                    }
                    break;
                default:
                    if(aggType == Type.INT_TYPE)
                        result += ((IntField)tp.getField(afield)).getValue();
                    else
                        str += ((StringField)tp.getField(afield)).getValue();
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
        if(what == Aggregator.Op.AVG) result /= pair.getValue().size();
        else if(what == Aggregator.Op.COUNT) result = pair.getValue().size();

        td = new TupleDesc(typeArr, fieldAr);
        Tuple ret = new Tuple(td);
        ret.setField(0, pair.getKey());
        if(aggType == Type.INT_TYPE || what == Aggregator.Op.COUNT)
            ret.setField(1, new IntField(result));
        else
            ret.setField(1, new StringField(str, str.length()));

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
