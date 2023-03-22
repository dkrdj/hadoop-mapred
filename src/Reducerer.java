package ssafy;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reducerer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Text value, Context context) throws IOException, InterruptedException {
        if (key.toString().equals("") || key.toString() == null) {
            throw new RuntimeException("뭔가 잘못됨");
        }
        context.write(key, value);
    }
}
