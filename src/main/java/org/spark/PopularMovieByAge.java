package org.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


public class PopularMovieByAge {
public static void popMoviesByAge(String fileName1, String fileName2, String fileName3, String outputDir){
    SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Popular-Movies-By-Age");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    JavaRDD<String> ratingData = sc.textFile(fileName1);
    JavaRDD<String> userData = sc.textFile(fileName2);
    JavaRDD<String> movieData = sc.textFile(fileName3);

    //User ID - Movie ID
    JavaPairRDD<String, String> reviewsByUserId = ratingData
            .mapToPair(t-> {
                String[] line = t.split("\\t");
                return new Tuple2<>(line[0], line[1]);
            });

    //User ID - Age
    JavaPairRDD<String, String> ageByUserId = userData
            .mapToPair( t->
            {
                String[] line = t.split("\\|");
                return new Tuple2<>(line[0], line[1]);
            });

    //Movie ID - Movie Name
    JavaPairRDD<String, String> movieById = movieData
            .mapToPair( t->
            {
                String[] line = t.split("\\|");
                return new Tuple2<>(line[0], line[1]);
            });


    JavaPairRDD joinData =
            reviewsByUserId.join(ageByUserId)
                    .mapToPair( x -> new Tuple2<>(new Tuple2<>(Integer.valueOf(x._2()._2), x._2()._1), 1))
                    .reduceByKey(Integer::sum)
                    .mapToPair(x-> new Tuple2<>(x._2, x._1))
                    .sortByKey(false)
                    .groupBy(x-> x._2._1)
                    .mapToPair(x-> new Tuple2<>(x._2.iterator().next()._2._2, x._1))
                    .join(movieById)
                    .mapToPair(x-> new Tuple2<>(x._2._1, x._2._2))
                    .sortByKey();


    joinData.saveAsTextFile(outputDir+"PopularMoviesByAge");

}
    public static void main(String[] args){
        if (args.length == 0) {
            System.out.println("No files provided.");
        }
        popMoviesByAge(args[0], args[1], args[2], args[3]);
    }
}
