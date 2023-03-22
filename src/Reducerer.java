package ssafy;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reducerer extends Reducer<Path, Text, Path, Text> {

    public void reduce(Path key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}
