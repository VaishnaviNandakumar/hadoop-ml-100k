package org.hadoop.avg.ratings;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AverageRatingsDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "job1");
        job1.setJarByClass(AverageRatingsDriver.class);
        job1.setMapperClass(AverageRatingsMapper.class);
        job1.setReducerClass(AverageRatingsReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(args[1]), true);
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf1, "job2");
        job2.setJarByClass(AverageRatingsDriver.class);
        job2.setMapperClass(KeySwapperMapperFloat.class);
        job2.setSortComparatorClass(FloatComparator.class);
        job2.setOutputKeyClass(FloatWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        fs.delete(new Path(args[2]), true);
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
