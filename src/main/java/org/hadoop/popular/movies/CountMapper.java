package org.hadoop.popular.movies;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private final static Text word = new Text();
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split("\t");
        String movieCode = data[1];
        word.set(movieCode);
        context.write(word, one);
    }
}
