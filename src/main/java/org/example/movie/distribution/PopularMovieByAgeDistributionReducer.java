package org.example.movie.distribution;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PopularMovieByAgeDistributionReducer extends Reducer<Text, Text, Text, Text> {
    Text result = new Text();
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String, Integer> map = new HashMap<>();
        int val;
        for(Text movie: values){
            if(map.get(movie.toString())!=null){
                val = map.get(movie.toString());
            }
            else{
                val = 0;
            }
            map.put(movie.toString(), val+1);
        }


        String maxCount = "";
        int maxValueInMap = (Collections.max(map.values()));
        for (Map.Entry<String, Integer> entry :
                map.entrySet()) {
            if (entry.getValue() == maxValueInMap) {

                maxCount = entry.getKey();
            }
        }
        result.set(maxCount);
        context.write(key, result);
    }
}
