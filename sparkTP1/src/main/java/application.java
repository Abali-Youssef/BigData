import org.apache.spark.SparkConf;


import org.apache.spark.SparkContext$;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.logging.Logger;

public class application {
    public static void main(String[] args) {

        SparkConf conf=new SparkConf().setAppName("etudiants").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        JavaRDD<String> rdd1=sc.parallelize(Arrays.asList("ahmed mobarak ali","akram youssef med","ali_2 youssef_2 akrm_2 med_2","mobarak_2 samir hakim"));
        System.out.println("********************   rdd _ 1    ****************");
        rdd1.foreach(
                a -> System.out.println(a)
        );
        JavaRDD<String> rdd_2=rdd1.flatMap(a -> Arrays.asList( a.split(" ")).iterator());

        JavaPairRDD<String,Integer> rdd2=rdd_2.mapToPair(s->new Tuple2<>(s,s.length()));
        System.out.println("********************   rdd _ 2    ****************");
        rdd2.foreach(
                a -> System.out.println(a)
        );
        JavaPairRDD<String,Integer> rdd3=rdd2.filter(val -> val._2()<8);
        System.out.println("********************   rdd _ 3    ****************");
        rdd3.foreach(
                a -> System.out.println(a)
        );
        JavaPairRDD<String,Integer> rdd4=rdd2.filter(val -> val._1().charAt(0)=='a');
        System.out.println("********************   rdd _ 4    ****************");
        rdd4.foreach(
                a -> System.out.println(a)
        );
        JavaPairRDD<String,Integer> rdd5=rdd2.filter(val -> val._2()>4);
        System.out.println("********************   rdd _ 5    ****************");
        rdd5.foreach(
                a -> System.out.println(a)
        );
        JavaPairRDD<String,Integer> rdd6=rdd3.union(rdd4);
        System.out.println("********************   rdd _ 6    ****************");
        rdd6.foreach(
                a -> System.out.println(a)
        );
        JavaPairRDD<String,Integer> rdd81=rdd6.mapToPair(s->new Tuple2<>(s._1(),s._2()-1));
        System.out.println("********************   rdd _ 81    ****************");
        rdd81.foreach(
                a -> System.out.println(a)
        );
        JavaPairRDD<String,Integer> rdd71 =rdd5.mapToPair(s->new Tuple2<>(s._1(),s._2()*2));
        System.out.println("********************   rdd _ 71    ****************");
        rdd71.foreach(
                a -> System.out.println(a)
        );
        JavaPairRDD<String,Integer> rdd7=rdd71.reduceByKey((val1,val2)->val1+val2 );
        System.out.println("********************   rdd _ 7    ****************");
        rdd7.foreach(
                a -> System.out.println(a)
        );
        JavaPairRDD<String,Integer> rdd8=rdd81.reduceByKey((val1,val2)->val1*val2 );
        System.out.println("********************   rdd _ 8    ****************");
        rdd8.foreach(
                a -> System.out.println(a)
        );
        JavaPairRDD<String,Integer> rdd9=rdd7.union(rdd8);
        System.out.println("********************   rdd _ 9    ****************");
        rdd9.foreach(
                a -> System.out.println(a)
        );
        JavaPairRDD<String,Integer> rdd10=rdd9.sortByKey();
        System.out.println("********************   rdd _ 10    ****************");
        rdd10.foreach(
                a -> System.out.println(a)
        );
    }
}
