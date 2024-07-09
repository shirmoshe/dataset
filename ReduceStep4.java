package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;

public class ReduceStep4 extends Reducer<LongWritable, Text, Text, LongWritable> {

    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        TreeSet<String> ts = new TreeSet<String>();
        for (Text value: values){
            ts.add(value.toString());
        }

        for (String value: ts){
            int count = -Integer.parseInt(key.toString());
            context.write(new Text(value.split("\\*")[1]), new LongWritable(count));
        }
    }
}
