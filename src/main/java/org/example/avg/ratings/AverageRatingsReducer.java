package org.example.avg.ratings;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AverageRatingsReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
    private static final FloatWritable avg = new FloatWritable();
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int ratingSum = 0;
        int countSum = 0;

        for(IntWritable val : values){
            ratingSum += val.get();
            countSum++;
        }

        avg.set((float) ratingSum /countSum);
        context.write(key, avg);
    }
}
