package com.edmundophie.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * Created by edmundophie on 12/15/15.
 */
public class ActualKeyPartitioner extends Partitioner<CompositeKey, IntWritable> {
//    private HashPartitioner<Text, IntWritable> hashPartitioner = new HashPartitioner<Text, IntWritable>();
//    private Text newKey = new Text();

    @Override
    public int getPartition(CompositeKey compositeKey, IntWritable values, int numPartitions) {
        int hash = compositeKey.getAuthor().hashCode();
        return hash%numPartitions;
    }
}
