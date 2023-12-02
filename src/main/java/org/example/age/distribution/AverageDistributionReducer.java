package org.example.age.distribution;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AverageDistributionReducer  extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable i: values){
            sum+=i.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
