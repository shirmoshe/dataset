לבוק אצל רז:
את הפוקנציית MAP בהתחלה אם היא זורקת שגיאות 




package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapStep1 extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String SID = line.split(",")[1];
        line = line.replaceFirst(","+SID,"");

        context.write(new Text(SID), new Text(line));
    }
}
