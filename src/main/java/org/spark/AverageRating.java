package org.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AverageRating {
    public static void avgRatingMovies(String fileName, String outputDir){
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Avg-Ratings-Movie-Distribution");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> inputFile = sc.textFile(fileName);

        JavaPairRDD avgRatings = inputFile
                .mapToPair(t-> {
                    String[] line = t.split("\\t");
                    return new Tuple2<>(line[1], new Tuple2<>(Float.valueOf(line[2]), 1));
                })
                .reduceByKey((x,y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
                .mapToPair(t -> new Tuple2<>(t._2()._1/t._2()._2, t._1))
                .sortByKey(false)
                .mapToPair(t -> new Tuple2<>(t._2, t._1));

        avgRatings.saveAsTextFile(outputDir+"AverageRatingsPopularMovies");
    }

    public static void main(String[] args){
        if (args.length == 0) {
            System.out.println("No files provided.");
        }

        avgRatingMovies(args[0], args[1]);
    }
}

