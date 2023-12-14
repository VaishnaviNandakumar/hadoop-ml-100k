package org.hadoop.age.distribution;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;


public class AverageDistributionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    IntWritable outputValue = new IntWritable();
    Text outputKey = new Text();
    HashMap<String, String> hashmap = new HashMap<>();
    @Override
    protected void setup(Context context) throws IOException {
        Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        for(Path file : files) {
            if (file.getName().equals("u.user")) {
                BufferedReader reader = new BufferedReader(new FileReader(file.toString()));
                String line = reader.readLine();
                while (line != null) {
                    String[] cols = line.split("\\|");
                    hashmap.put(cols[0], cols[1]);
                    line = reader.readLine();
                }
            }
        }
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split("\t");
        if(hashmap.get(data[0])!=null){
            outputKey.set(hashmap.get(data[0]));
            outputValue.set(1);
            context.write(outputKey, outputValue);
        }
    }
}
