package org.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AgeDistribution {
    public static void ageDistributionMovies(String fileName1, String fileName2, String outputDir){
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Age-Movie-Distribution");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> ratingData = sc.textFile(fileName1);
        JavaRDD<String> userData = sc.textFile(fileName2);

        JavaPairRDD<String, Integer> reviewsByUserId = ratingData
                .mapToPair(t-> {
                    String[] line = t.split("\\t");
                    return new Tuple2<>(line[0], 1);
                })
                .reduceByKey(Integer::sum);

        JavaPairRDD<String, String> ageByUserId = userData
                .mapToPair( t->
                {
                    String[] line = t.split("\\|");
                    return new Tuple2<>(line[0], line[1]);
                });

        JavaPairRDD joinData =
                reviewsByUserId.join(ageByUserId)
                        .mapToPair( x -> new Tuple2<>(Integer.valueOf(x._2()._2), x._2()._1))
                        .reduceByKey(Integer::sum)
                        .sortByKey();

        joinData.saveAsTextFile(outputDir+"AgeDistributionData");

    }

    public static void main(String[] args){
        if (args.length == 0) {
            System.out.println("No files provided.");
        }
        ageDistributionMovies(args[0], args[1], args[2]);
    }
}
