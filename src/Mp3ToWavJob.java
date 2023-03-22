package ssafy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Mp3ToWavJob {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "Mp3ToWav");

        job.setJarByClass(Mp3ToWavJob.class);
        job.setMapperClass(Mp3FileMapper.class);
        job.setInputFormatClass(Mp3InputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(5);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
//        FileSystem fs = FileSystem.get(conf);
//        if (fs.exists(new Path(args[1]))) {
//            fs.delete(new Path(args[1]), true);
//        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}