package wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapStep2 extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String removeString = line.split("\\[")[0];
        line = line.replaceFirst(removeString+"\\[","").replaceAll("\\]","");
        String SID = removeString.split("\t")[0];

        context.write(new Text(SID), new Text(line));
    }
}
