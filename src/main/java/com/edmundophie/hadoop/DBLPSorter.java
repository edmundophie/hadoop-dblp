package com.edmundophie.hadoop;

import com.edmundophie.dblp.Parser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by edmundophie on 12/15/15.
 */
public class DBLPSorter {

    public static class SortMapper extends Mapper<Object, Text, IntWritable, Text> {
        private CompositeKey ck = new CompositeKey();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] temp = value.toString().split("\\t");
//            ck.setAuthor(temp[0]);
//            ck.setPublicationQty(Integer.parseInt(temp[1]));
            context.write(new IntWritable(Integer.parseInt(temp[1])), new Text(temp[0]));
        }
    }

//    public static class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
////        private IntWritable resultVal = new IntWritable();
//
//        @Override
//        public void reduce(IntWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
////            int sum = 0;
////            for(IntWritable val:values) {
////                sum += val.get();
////            }
////            resultVal.set(sum);
////            key.setPublicationQty(sum);
//            context.write(value.iterator().next(), key);
//        }
//    }
}
