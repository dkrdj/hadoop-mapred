package ssafy;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.nio.ByteBuffer;

public class Mp3FileMapper extends Mapper<Path, BytesWritable, Path, Text> {

    public void map(Path key, BytesWritable value, Context context) throws IOException, InterruptedException {

        // HDFS Configuration 설정
        String src = key.toUri().toURL().toString().split("music/")[1];

        File file = new File(src);
        if (!file.exists()) {
            new File(src.split("/")[0]).mkdirs();
            file.createNewFile();
        }
        FileOutputStream outputStream = new FileOutputStream(file);

        outputStream.write(value.getBytes()); // byte 배열을 파일에 기록합니다.

        outputStream.close(); // 스트림을 닫습니다.

        BufferedInputStream in = new BufferedInputStream(new FileInputStream(src));
        ByteBuffer buffer = ByteBuffer.allocate(in.available() + 1);
        in.read(buffer.array());
        in.close();
        BytesWritable output = new BytesWritable();
        output.set(buffer.array(), 0, buffer.array().length);

        context.write(key, new Text(src));
    }
}