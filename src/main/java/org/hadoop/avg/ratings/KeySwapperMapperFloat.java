package org.hadoop.avg.ratings;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KeySwapperMapperFloat extends Mapper<Object, Text, FloatWritable, Text> {
    FloatWritable frequency = new FloatWritable();
    Text t = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split("\t");
        frequency.set(Float.parseFloat(data[1]));
        t.set(data[0]);
        context.write(frequency, t);
    }
}
