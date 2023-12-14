package org.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


public class PopularMovies {
    public static void popularMovies(String fileName, String outputDir){
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Popular-Movie-Distribution");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> inputFile = sc.textFile(fileName);
        JavaPairRDD pairRDD = inputFile
                .mapToPair(t-> new Tuple2<>(t.split("\\t")[1],1))
                .reduceByKey(Integer::sum)
                .mapToPair(x -> new Tuple2<>(x._2, x._1))
                .sortByKey(false);
        pairRDD.saveAsTextFile(outputDir+"PopularMovies");
    }
    public static void main(String[] args){
        if (args.length == 0) {
            System.out.println("No files provided.");
        }

        popularMovies(args[0], args[1]);
    }
}
