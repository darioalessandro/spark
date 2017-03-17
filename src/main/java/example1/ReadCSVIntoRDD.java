package example1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class ReadCSVIntoRDD {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ReadCSVIntoRDD");
        JavaSparkContext sc = new JavaSparkContext(conf);
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        String path = classloader.getResource("example1.csv").getPath();
        JavaRDD<String> rdd = sc.textFile(path);
        List<String> results = rdd.collect();
        for(String s : results) System.out.println(s);
    }
}
