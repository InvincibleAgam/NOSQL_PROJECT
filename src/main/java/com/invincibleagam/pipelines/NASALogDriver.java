package com.invincibleagam.pipelines;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NASALogDriver {

    public static class Q1Mapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outVal = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length >= 9) {
                outKey.set(parts[2] + "\t" + parts[7]);
                outVal.set("1\t" + parts[8]);
                context.write(outKey, outVal);
            }
        }
    }

    public static class Q1Reducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int requestCount = 0;
            long totalBytes = 0;
            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                requestCount += Integer.parseInt(parts[0]);
                totalBytes += Long.parseLong(parts[1]);
            }
            result.set(requestCount + "\t" + totalBytes);
            context.write(key, result);
        }
    }

    public static class Q2Mapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outVal = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length >= 9) {
                outKey.set(parts[5]);
                outVal.set("1\t" + parts[8] + "\t" + parts[0]);
                context.write(outKey, outVal);
            }
        }
    }

    public static class Q2Reducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int requestCount = 0;
            long totalBytes = 0;
            HashSet<String> hosts = new HashSet<>();
            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                requestCount += Integer.parseInt(parts[0]);
                totalBytes += Long.parseLong(parts[1]);
                hosts.add(parts[2]);
            }
            result.set(requestCount + "\t" + totalBytes + "\t" + hosts.size());
            context.write(key, result);
        }
    }

    public static class Q3Mapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outVal = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length >= 9) {
                int statusCode = Integer.parseInt(parts[7]);
                int isError = (statusCode >= 400 && statusCode <= 599) ? 1 : 0;
                outKey.set(parts[2] + "\t" + parts[3]);
                outVal.set(isError + "\t" + parts[0]);
                context.write(outKey, outVal);
            }
        }
    }

    public static class Q3Reducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalCount = 0;
            int errorCount = 0;
            HashSet<String> errorHosts = new HashSet<>();
            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                int isError = Integer.parseInt(parts[0]);
                totalCount++;
                if (isError == 1) {
                    errorCount++;
                    errorHosts.add(parts[1]);
                }
            }
            double errorRate = totalCount > 0 ? (double) errorCount / totalCount : 0.0;
            result.set(errorCount + "\t" + totalCount + "\t" + errorRate + "\t" + errorHosts.size());
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.exit(2);
        }
        String queryId = args[0];
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "NASA Log Analysis - " + queryId);
        job.setJarByClass(NASALogDriver.class);

        if (queryId.equals("q1")) {
            job.setMapperClass(Q1Mapper.class);
            job.setReducerClass(Q1Reducer.class);
        } else if (queryId.equals("q2")) {
            job.setMapperClass(Q2Mapper.class);
            job.setReducerClass(Q2Reducer.class);
        } else if (queryId.equals("q3")) {
            job.setMapperClass(Q3Mapper.class);
            job.setReducerClass(Q3Reducer.class);
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean success = job.waitForCompletion(true);
        if (!success) {
            throw new Exception("Hadoop Job Failed");
        }
    }
}
