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

/**
 * Author: Jiankai Dang
 * Date: 12/6/13
 */

public class Job2 {
    public static void main(String[] args) throws Exception {
        Job job = new Job(new Configuration());
        job.setJarByClass(Job2.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        int n = Integer.parseInt(args[0]);

        FileInputFormat.addInputPath(job, new Path("Job2/loop" + (n - 1) + "/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("Job2/loop" + n + "/"));

        job.waitForCompletion(true);
    }

    public static class Map extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);

            String[] node = value.toString().split("\t");
            int len = node.length;
            double p = Double.parseDouble(node[0]) / (len - 1);

            for (int i = 1; i < len; i++) {
                context.write(new Text(node[i]), new Text(String.valueOf(p)));
            }
        }
    }

    private static class Combine extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String adjacencyList = "";
            double p = 0;

            for (Text value : values) {
                String nodeStr = value.toString();
                String[] node = nodeStr.split("\t");
                if (node.length > 1) {
                    adjacencyList = nodeStr.substring(nodeStr.indexOf("\t"));
                } else {
                    p += Double.parseDouble(nodeStr);
                }
            }

            context.write(key, new Text(p + adjacencyList));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String adjacencyList = "";
            double p = 0;

            for (Text value : values) {
                String nodeStr = value.toString();
                String[] node = nodeStr.split("\t");
                if (node.length > 1) {
                    adjacencyList = nodeStr.substring(nodeStr.indexOf("\t"));
                }
                p += Double.parseDouble(node[0]);
            }

            context.write(key, new Text((0.15 + 0.85 * p) + adjacencyList));
        }
    }
}
