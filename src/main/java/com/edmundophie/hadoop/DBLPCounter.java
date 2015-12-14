package com.edmundophie.hadoop;

import com.edmundophie.dblp.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by edmundophie on 12/14/15.
 */
public class DBLPCounter {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final static Text ARTICLE_TEXT = new Text("article");
        private final static Text INPROCEEDINGS_TEXT = new Text("inproceedings");
        private final static Text MASTERTHESIS_TEXT = new Text("masterthesis");
        private final static Text PHDTHESIS_TEXT = new Text("phdthesis");

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Parser parser = new Parser(value.toString());
            context.write(ARTICLE_TEXT, parser.getArticleCount());
            context.write(INPROCEEDINGS_TEXT, parser.getInproceedingsCount());
            context.write(MASTERTHESIS_TEXT, parser.getMasterThesisCount());
            context.write(PHDTHESIS_TEXT, parser.getPhdThesisCount());
            for(String authorName:parser.getAuthorNames()) {
                context.write(new Text(authorName), one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable resultVal = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val:values) {
                sum += val.get();
            }
            resultVal.set(sum);
            context.write(key, resultVal);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "dblp counter");
        job.setJarByClass(DBLPCounter.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
