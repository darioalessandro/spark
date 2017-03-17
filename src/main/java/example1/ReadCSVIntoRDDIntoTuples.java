package example1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class ReadCSVIntoRDDIntoTuples {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ReadCSVIntoRDDIntoPairRDD");
        JavaSparkContext sc = new JavaSparkContext(conf);
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        String path = classloader.getResource("example1.csv").getPath();
        JavaRDD<String> rdd = sc.textFile(path); //"robert,1 -> Tuple(String,Integer)
        JavaPairRDD<String,Integer> pairRDD = rdd.mapToPair(new PairFunction<String, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(String s) throws Exception {
                    String[] columns = s.split(",");
                    String name = columns[0];
                    Integer integer = new Integer(columns[1]);
                    return new Tuple2<String,Integer>(name, integer);
                }
            }
        );

//        JavaPairRDD < String, Integer > pairRDD = rdd.mapToPair(row -> {
//            String[] columns = row.split(",");
//            String name = columns[0];
//            Integer integer = new Integer(columns[1]);
//            return new Tuple2<>(name, integer);
//        });
//        List<Tuple2<String,Integer>> results = pairRDD.filter(new Function<Tuple2<String, Integer>, Boolean>() {
//            @Override
//            public Boolean call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                return stringIntegerTuple2._1().equalsIgnoreCase("robert");
//            }
//        }).collect();

        List<Tuple2<String,Integer>> results = pairRDD.filter(t -> t._1().equalsIgnoreCase("robert")).collect();

        for(Tuple2<String,Integer> s : results) System.out.println("name "+ s._1() + ",count " + s._2());
    }
}
