import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.RootLogger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Exercice2 {
    public static void main(String[] args) {
        RootLogger rootLogger = (RootLogger) Logger.getRootLogger();
        rootLogger.setLevel(Level.ERROR);
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.spark-project").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("TP7-EXercice2").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Question 1
        JavaRDD<String> rdd0 = sc.textFile("ventes.txt");
        JavaPairRDD<String,Integer> rddMap = rdd0.mapToPair(s-> new Tuple2<>(s.split(" ")[1],Integer.parseInt(s.split(" ")[3])));
        System.out.println("Question 1 : "+rddMap.collect());

        JavaPairRDD<String,Integer> rddReduce=rddMap.reduceByKey((v1, v2) -> v1+v2);
        System.out.println("Sorted : "+rddReduce.collect());

        // Question 2
        JavaPairRDD<String,Integer> rddMap2 = rdd0
                .mapToPair(s-> new Tuple2<>("Total des ventes en "+s.split(" ")[0]+"  dans la ville "
                        +s.split(" ")[1],Integer.parseInt(s.split(" ")[3])));
        System.out.println("Question 2 : "+rddMap2.collect());

        JavaPairRDD<String,Integer> rddReduce2=rddMap2.reduceByKey((v1, v2) -> v1+v2);
        System.out.println("Sorted : "+rddReduce2.collect());
    }
}
