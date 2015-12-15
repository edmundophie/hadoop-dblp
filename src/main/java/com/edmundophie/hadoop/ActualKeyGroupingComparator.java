package com.edmundophie.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by edmundophie on 12/15/15.
 */
public class ActualKeyGroupingComparator extends WritableComparator {
    protected ActualKeyGroupingComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        CompositeKey key1 = (CompositeKey) w1;
        CompositeKey key2 = (CompositeKey) w2;
        return key1.getAuthor().compareTo(key2.getAuthor());
    }
}
