package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

    public class MapStep4 extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        int count = -Integer.parseInt(line.split("\t")[1]);

        String word = line.split("\t")[0];
        String[] splitword = word.split(" ");
        splitword[0] = splitword[0].split(",")[0];
        splitword[1] = splitword[1].split(",")[0];
        line = String.join(",", splitword[0], splitword[1]);
        line = String.join("*", line, word);

        context.write(new LongWritable(count), new Text(line));
    }
}
