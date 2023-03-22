package ssafy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.nio.ByteBuffer;

public class Wordcount {
    /* Main function */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "word count");
        job.setJarByClass(Wordcount.class);

        // let hadoop know my map and reduce classes
        job.setMapperClass(TokenizerMapper.class);
//        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // set number of reduces
        job.setNumReduceTasks(0);

        // set input and output directories
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /*
    Object, Text : input key-value pair type (always same (to get a line of input file))
    Text, IntWritable : output key-value pair type
    */
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        // variable declairations
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        // map function (Context -> fixed parameter)
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // value.toString() : get a line
            String inputSrc = "hdfs://ip-172-26-0-222.ap-northeast-2.compute.internal:9000/user/j8a603/music/" + value.toString();
            Path inFile = new Path(inputSrc);
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream inputStream = fs.open(inFile);
            ByteBuffer buffer = ByteBuffer.allocate(inputStream.available());
            inputStream.read(buffer.array());
            inputStream.close();

            String src = value.toString();
            File file = new File(src);
            if (!file.exists()) {
                new File(src.split("/")[0]).mkdirs();
                file.createNewFile();
            }


            FileOutputStream localOutput = new FileOutputStream(file);
            localOutput.write(buffer.array());
            localOutput.close();

            BufferedInputStream in = new BufferedInputStream(new FileInputStream("Ben - 끝까지.mp3"));
            ByteBuffer localBuffer = ByteBuffer.allocate(in.available());
            in.read(localBuffer.array());
            in.close();


            String outputSrc = "hdfs://ip-172-26-0-222.ap-northeast-2.compute.internal:9000/user/j8a603/out/" + value.toString();
            Path outFile = new Path(outputSrc);
            FSDataOutputStream outputStream = fs.create(outFile);
            outputStream.write(localBuffer.array());
            outputStream.close();
            word.set(inputSrc);
            context.write(word, one);
        }
    }

    /*
    Text, IntWritable : input key type and the value type of input value list
    Text, IntWritable : output key-value pair type
    */
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        // variables
        private IntWritable result = new IntWritable();

        // key : a disticnt word
        // values :  Iterable type (data list)
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}

