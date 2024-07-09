
לבדוק אם לרז יש את אותו main 
לבדוק איך רז מחכה לג'ובים שיסתיימו 


package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Random;

public class Main extends Configured implements Tool {

    // ------------- DRIVER ------------
    @Override
    public int run(String[] args) throws Exception {
        Path tempDir1 = new Path("data/temp-1");
        Path tempDir2 = new Path("data/temp-2");
        Path tempDir3 = new Path("data/temp-3");
        Configuration conf = getConf();
        FileSystem.get(conf).delete(new Path("data/output"), true);
        FileSystem.get(conf).delete(tempDir1, true);
        FileSystem.get(conf).delete(tempDir2, true);
        FileSystem.get(conf).delete(tempDir3, true);

        try {
            System.out.println("-------STEP 1-------");
            Job step1Job = Job.getInstance(conf);
            step1Job.setJobName("step1");
            step1Job.setJarByClass(Main.class);

            step1Job.setMapOutputValueClass(Text.class);
            step1Job.setOutputKeyClass(Text.class);
            step1Job.setOutputValueClass(Text.class);

            step1Job.setMapperClass(MapStep1.class);
            step1Job.setReducerClass(ReduceStep1.class);

            FileInputFormat.addInputPath(step1Job, new Path(args[0]));
            FileOutputFormat.setOutputPath(step1Job, tempDir1);

            if (!step1Job.waitForCompletion(true)) {
                return 1;
            }

            System.out.println("-------STEP 2-------");
            conf = new Configuration();
            Job step2Job = Job.getInstance(conf);
            step2Job.setJobName("step2");

            step2Job.setJarByClass(Main.class);
            FileInputFormat.setInputPaths(step2Job, tempDir1);
            FileOutputFormat.setOutputPath(step2Job, tempDir2);

            step2Job.setMapOutputValueClass(Text.class);
            step2Job.setOutputKeyClass(Text.class);
            step2Job.setOutputValueClass(Text.class);

            step2Job.setMapperClass(MapStep2.class);
            step2Job.setReducerClass(ReduceStep2.class);

            if (!step2Job.waitForCompletion(true)) {
                return 1;
            }

            System.out.println("-------STEP 3-------");
            conf = new Configuration();
            Job step3Job = Job.getInstance(conf);
            step3Job.setJobName("step3");

            step3Job.setJarByClass(Main.class);
            FileInputFormat.setInputPaths(step3Job, tempDir2);
            FileOutputFormat.setOutputPath(step3Job, tempDir3);

            step3Job.setMapOutputValueClass(IntWritable.class);
            step3Job.setOutputKeyClass(Text.class);
            step3Job.setOutputValueClass(LongWritable.class);

            step3Job.setMapperClass(MapStep3.class);
            step3Job.setReducerClass(ReduceStep3.class);

            if (!step3Job.waitForCompletion(true)) {
                return 1;
            }

            System.out.println("-------STEP 4-------");
            conf = new Configuration();
            Job step4Job = Job.getInstance(conf);
            step4Job.setJobName("step4");

            step4Job.setJarByClass(Main.class);
            FileInputFormat.setInputPaths(step4Job, tempDir3);
            FileOutputFormat.setOutputPath(step4Job, new Path(args[1]));

            step4Job.setMapOutputValueClass(Text.class);
            step4Job.setMapOutputKeyClass(LongWritable.class);
            step4Job.setOutputKeyClass(Text.class);
            step4Job.setOutputValueClass(LongWritable.class);

            step4Job.setMapperClass(MapStep4.class);
            step4Job.setReducerClass(ReduceStep4.class);

            return step4Job.waitForCompletion(true) ? 0 : 1;

        } finally {
            FileSystem.get(conf).delete(tempDir1, true);
            FileSystem.get(conf).delete(tempDir2, true);
            FileSystem.get(conf).delete(tempDir3, true);
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Main(), args);
        System.exit(exitCode);
    }
}


