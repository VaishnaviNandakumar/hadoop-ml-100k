package org.hadoop.avg.ratings;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AverageRatingsMapper extends Mapper<Object, Text, Text, IntWritable> {
    private static final Text movie = new Text();
    private static final IntWritable rating = new IntWritable();

    public void map(Object key,Text value, Context context) throws IOException, InterruptedException{
        String[] data = value.toString().split("\t");
        String movieCode = data[1];
        movie.set(movieCode);
        rating.set(Integer.parseInt(data[2]));
        context.write(movie, rating);
    }
}
