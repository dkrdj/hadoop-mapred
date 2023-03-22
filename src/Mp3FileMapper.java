package ssafy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.ByteBuffer;

public class Mp3FileMapper extends Mapper<Path, BytesWritable, Path, BytesWritable> {

    @Override
    public void map(Path key, BytesWritable value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Path inFile = new Path("hdfs://ip-172-26-0-222.ap-northeast-2.compute.internal:9000/user/j8a603/music/경서/경서 - 밤 하늘의 별을(2020).mp3");
        try (FSDataInputStream inputStream = fs.open(inFile)) {
            // read the file contents
            // process the data and emit key-value pairs
            // using context.write()
            ByteBuffer buffer = ByteBuffer.allocate(inputStream.available());
            inputStream.read(buffer.array());
            Path outFile = new Path("hdfs://ip-172-26-0-222.ap-northeast-2.compute.internal:9000/user/j8a603/out/경서 - 밤 하늘의 별을(2020).mp3");
            FSDataOutputStream outputStream = fs.create(outFile);
            outputStream.write(buffer.array());
        }
        // HDFS Configuration 설정
//        String src = key.toUri().toURL().toString().split("music/")[1];
//
//        File file = new File(src);
//        if (!file.exists()) {
//            new File(src.split("/")[0]).mkdirs();
//            file.createNewFile();
//        }
//        FileOutputStream outputStream = new FileOutputStream(file);
//        outputStream.write(value.getBytes()); // byte 배열을 파일에 기록합니다.
//
//        outputStream.close(); // 스트림을 닫습니다.
//
//        BufferedInputStream in = new BufferedInputStream(new FileInputStream(src));
//        ByteBuffer buffer = ByteBuffer.allocate(in.available() + 1);
//        in.read(buffer.array());
//        in.close();
//        BytesWritable output = new BytesWritable();
//        output.set(buffer.array(), 0, buffer.array().length);

        context.write(key, value);
    }
}