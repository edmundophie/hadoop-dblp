package com.edmundophie.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by edmundophie on 12/15/15.
 */
public class CompositeKeyComparator extends WritableComparator {
    protected CompositeKeyComparator() {
        super(IntWritable.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        IntWritable ck1 = (IntWritable) w1;
        IntWritable ck2 = (IntWritable) w2;

//        int compare = ck1.getAuthor().compareTo(ck1.getAuthor());
//        if(compare == 0) {
//            int a = ck1.getPublicationQty();
//            int b = ck2.getPublicationQty();
//            return a<b?-1:(a==b?0:1);
//        }
        return ck2.compareTo(ck1);
    }
}
