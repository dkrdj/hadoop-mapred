package ssafy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Map;

public class Wordcount extends Configured implements Tool {
    static {
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");
        Configuration.addDefaultResource("yarn-default.xml");
        Configuration.addDefaultResource("yarn-site.xml");
        Configuration.addDefaultResource("mapred-default.xml");
        Configuration.addDefaultResource("mapred-site.xml");
    }

    /* Main function */
    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run(new Wordcount(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: <in> <out>");
            System.err.println("에러" + Arrays.toString(args));
            System.exit(2);
        }
        System.err.println(Arrays.toString(args));

        Configuration conf = getConf();
        for (Map.Entry<String, String> entry : conf) {
            System.err.println(entry.getKey() + " " + entry.getValue());
        }

        Job job = new Job(conf, "word count");
        job.setJarByClass(Wordcount.class);

        // let hadoop know my map and reduce classes
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // set number of reduces
        job.setNumReduceTasks(10);
        // set input and output directories
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /*
    Object, Text : input key-value pair type (always same (to get a line of input file))
    Text, IntWritable : output key-value pair type
    */
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        // variable declairations
        private final static IntWritable one = new IntWritable(1);

        // map function (Context -> fixed parameter)
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] split = value.toString().split("\n");
            for (String str : split) {
                Text v = new Text();
                Text k = new Text();
                v.set(str);
                k.set(str.split("/")[0]);
                // emit a key-value pair
                context.write(k, v);
            }
        }
    }

    /*
    Text, IntWritable : input key type and the value type of input value list
    Text, IntWritable : output key-value pair type
    */
    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
//                String inputSrc = "hdfs://ip-172-26-0-222.ap-northeast-2.compute.internal:9000/user/j8a603/music/" + value;
//                Path inFile = new Path(inputSrc);
//                Configuration conf = context.getConfiguration();
//                FileSystem fs = FileSystem.get(conf);
//                FSDataInputStream inputStream = fs.open(inFile);
//                ByteBuffer buffer = ByteBuffer.allocate(inputStream.available());
//                inputStream.read(buffer.array());
//                inputStream.close();
//
//                String src = value.toString();
//                File file = new File(src);
//                if (!file.exists()) {
//                    new File(src.split("/")[0]).mkdirs();
//                    file.createNewFile();
//                }
//
//
//                FileOutputStream localOutput = new FileOutputStream(file);
//                localOutput.write(buffer.array());
//                localOutput.close();
//
                //파이썬 코드 실행
//                ProcessBuilder pb = new ProcessBuilder("python3", "test.py");
//                pb.redirectErrorStream(true);
//                Process process = pb.start();
//                int exitval = process.waitFor();
//
//                BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
//
//                String line = br.readLine();

//                파이썬 코드 종료
                InetAddress ip = InetAddress.getLocalHost();
//                File newFile = new File(src);
//
//                FileInputStream in = new FileInputStream(newFile);
//                ByteBuffer localBuffer = ByteBuffer.allocate(in.available());
//                in.read(localBuffer.array());
//                in.close();
//
//
//                String outputSrc = "hdfs://ip-172-26-0-222.ap-northeast-2.compute.internal:9000/user/j8a603/out/" + value;
//                Path outFile = new Path(outputSrc);
//                FSDataOutputStream outputStream = fs.create(outFile);
//                outputStream.write(localBuffer.array());
//                outputStream.close();
                context.write(key, new Text(ip.getHostAddress()));
            }
        }
    }
}