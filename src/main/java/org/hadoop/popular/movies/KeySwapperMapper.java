package org.hadoop.popular.movies;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
public class KeySwapperMapper extends Mapper<Object, Text, IntWritable, Text> {
    IntWritable frequency = new IntWritable();
    Text t = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split("\t");
        frequency.set(Integer.parseInt(data[1]));
        t.set(data[0]);
        context.write(frequency, t);
    }
}

