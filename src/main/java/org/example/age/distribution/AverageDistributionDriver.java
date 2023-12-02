package org.example.age.distribution;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class AverageDistributionDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "job1");
        job1.setJarByClass(AverageDistributionDriver.class);
        job1.setMapperClass(AverageDistributionMapper.class);
        job1.setReducerClass(AverageDistributionReducer.class);
        DistributedCache.addCacheFile(new URI(args[1]), job1.getConfiguration());
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(args[2]), true);
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        job1.waitForCompletion(true);
    }
}
