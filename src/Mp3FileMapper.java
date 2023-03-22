package ssafy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.InputStream;

public class Mp3FileMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        // HDFS Configuration 설정
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        // HDFS에서 파일 경로 가져오기
        Path path = new Path(value.toString());
        InputStream inputStream = fs.open(path);

        // .mp3 파일을 바이트 배열로 변환하여 맵리듀스 출력으로 내보내기
        byte[] bytes = new byte[inputStream.available()];
        inputStream.read(bytes);
        context.write(new Text(key.toString()), new Text(value.toString()));
//        context.write(value, new BytesWritable(bytes));

        // 입력 스트림 닫기
        inputStream.close();
    }
}