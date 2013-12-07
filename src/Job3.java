import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.TreeMap;

/**
 * Author: Jiankai Dang
 * Date: 12/6/13
 */

public class Job3 {
    public static void main(String[] args) throws Exception {
        Job job = new Job(new Configuration());
        job.setJarByClass(Job3.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("Job2/loop10/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path(args[0] + "/top100"));

        job.waitForCompletion(true);
    }

    public static class Map extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] node = value.toString().split("\t");
            context.write(key, new Text(node[0]));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        TreeMap<Double, String> priorityQueue = new TreeMap<Double, String>();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            priorityQueue.put(Double.parseDouble(values.iterator().next().toString()), key.toString());
            if (priorityQueue.size() > 100) {
                priorityQueue.pollFirstEntry();
            }
        }

        protected void cleanup(Context context)
                throws IOException,
                InterruptedException {
            while (!priorityQueue.isEmpty()) {
                java.util.Map.Entry<Double, String> entry = priorityQueue.pollLastEntry();
                context.write(new Text(entry.getValue()), new Text(entry.getKey().toString()));
            }
        }
    }
}
