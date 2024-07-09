package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapStep3 extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String removeString = line.split("\t")[0];
        line = line.replaceFirst(removeString+"\t","");
        String a = line.substring(line.length()-1);
        if (line.substring(line.length()-1).equals("1")){
            line = line.substring(0,line.lastIndexOf(" "));
            context.write(new Text(line), new IntWritable(1));
        }
    }
}
