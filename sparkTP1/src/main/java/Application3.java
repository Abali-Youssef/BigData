import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Application3 {
    public static void main(String[] args) {

        SparkConf conf=new SparkConf().setAppName("ventes").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");
        JavaRDD<String> data=sc.textFile("ventes.txt");
        JavaPairRDD<String,Integer> line=data.mapToPair(s->new Tuple2<>(Arrays.asList(s.split(" ")).get(1) ,1));
        JavaPairRDD<String,Integer> result=line.reduceByKey((v1, v2)->v1+v2);
        System.out.println("resultat :");
        result.foreach(
                e-> System.out.println(e)
        );
    }
}
