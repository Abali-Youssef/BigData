import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class OccurencesMapper extends MapReduceBase
        implements Mapper<LongWritable, Text,Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String val[]=value.toString().split(" ");
            //question 2
           //output.collect(new Text(val[1]+" "+ val[0]),new IntWritable(Integer.valueOf(val[3])));
        output.collect(new Text(val[1]),new IntWritable(Integer.valueOf(val[3])));
    }
}