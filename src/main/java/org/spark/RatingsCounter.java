package org.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


public class RatingsCounter {
    public static void ratingsCounter(String fileName, String outputdir){
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Ratings Counter");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> inputFile = sparkContext.textFile(fileName);
        JavaPairRDD data = inputFile.mapToPair(t-> new Tuple2(t.split("\\t")[2],1))
                .reduceByKey((x, y) -> (int) x + (int) y)
                .sortByKey();
        data.saveAsTextFile(outputdir+"CountData");
    }

    public static void main(String[] args){
        if (args.length == 0) {
            System.out.println("No files provided.");
        }

        ratingsCounter(args[0], args[1]);
    }
}
