בלולאת פור אפשר לנסות לעשות קוגם ווליו טו סטרינג ורק אחכ להכניס לעץ


package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;

public class ReduceStep1 extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        TreeSet<String> ts = new TreeSet<String>();
        for (Text value: values){
            ts.add(value.toString());
        }

        context.write(key, new Text(ts.toString()));
    }
}
