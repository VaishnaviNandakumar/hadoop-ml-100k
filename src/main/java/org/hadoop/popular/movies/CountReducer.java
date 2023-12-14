package org.hadoop.popular.movies;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static final IntWritable result = new IntWritable(0);
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable i: values){
            sum+=i.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
