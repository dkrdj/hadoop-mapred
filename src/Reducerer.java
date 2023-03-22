package ssafy;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reducerer extends Reducer<Path, BytesWritable, Path, Text> {

    public void reduce(Path key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(key, new Text("1231"));
    }
}
