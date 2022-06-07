import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class OccurencesMapper extends MapReduceBase
        implements Mapper<LongWritable, Text,Text, DoubleWritable> {

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        String val[]=value.toString().split(",");
        //output.collect(new Text(val[2]),new DoubleWritable(Double.valueOf(val[4])));
        utput.collect(new Text(val[2]),new DoubleWritable(Double.valueOf("1")));
    }
}