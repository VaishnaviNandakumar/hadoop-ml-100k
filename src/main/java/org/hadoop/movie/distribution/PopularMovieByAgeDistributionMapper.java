package org.hadoop.movie.distribution;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;


public class PopularMovieByAgeDistributionMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text outputKey = new Text();
    Text outputValue = new Text();
    HashMap<String, String> hashmap = new HashMap<>();
    HashMap<String, String> movieHashmap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException{
//        Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
//        for(Path file : files) {
        Path file= new Path("/Users/vaishnavink/Desktop/hdp/src/main/resources/movie-data-input/u.user");
            if (file.getName().equals("u.user")) {
                BufferedReader reader = new BufferedReader(new FileReader(file.toString()));
                String line = reader.readLine();
                while (line != null) {
                    String[] cols = line.split("\\|");
                    hashmap.put(cols[0], cols[1]);
                    line = reader.readLine();
                }
//            }

            file = new Path("/Users/vaishnavink/Desktop/hdp/src/main/resources/movie-data-input/u.item");
            if (file.getName().equals("u.item")) {
                reader = new BufferedReader(new FileReader(file.toString()));
                line = reader.readLine();
                while (line != null) {
                    String[] cols = line.split("\\|");
                    movieHashmap.put(cols[0], cols[1]);
                    line = reader.readLine();
                }
            }
        }
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split("\t");
        if(hashmap.get(data[0])!=null){
            int age = Integer.parseInt(hashmap.get(data[0]));
            int category = age/10;
            String keyValue = (category*10) + " - " + (category+1)*10;
            outputKey.set(String.valueOf(age));
            outputValue.set(movieHashmap.get(data[1]));
            context.write(outputKey, outputValue);
        }
    }
}
