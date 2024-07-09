package wordcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;

public class ReduceStep2 extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        //String[] line = values.toString().split(", ");
        for (Text value: values) {
            String line = value.toString();
            String[] lines = line.split(", ");
            if (lines.length < 3){
                String PP1 = lines[0].split(",", 2)[1];
                String PP2;
                if(lines.length == 2){
                    PP2 = lines[1].split(",", 2)[1];
                } else{
                    PP2 = String.join(",",PP1.split(",")[0],"0");
                }
                String PP3 = String.join(",",PP2.split(",")[0],"0");
                context.write(key, new Text(String.join(" ",PP1,PP2,PP3)));
            }
            String a = lines[0];
            for (int i = 0; i <= lines.length-3; i++){
                String PP1 = lines[i].split(",", 2)[1];
                String PP2 = lines[i+1].split(",", 2)[1];
                String PP3 = lines[i+2].split(",", 2)[1];
                context.write(key, new Text(String.join(" ",PP1,PP2,PP3)));
            }
        }
    }
}
