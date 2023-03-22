package ssafy;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;

public class Mp3InputFormat extends FileInputFormat<Path, BytesWritable> {
    public Mp3InputFormat() {
    }

    public RecordReader<Path, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        Mp3RecordReader reader = new Mp3RecordReader();
        reader.initialize(split, context);
        return reader;
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

    private static class Mp3RecordReader extends RecordReader<Path, BytesWritable> {
        private Path path;
        private BytesWritable value = new BytesWritable();
        private boolean processed = false;

        public Mp3RecordReader() {
        }

        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            path = ((FileSplit) split).getPath();
        }

        public boolean nextKeyValue() throws IOException, InterruptedException {

            if (!processed) {
                URL url;
                try {
                    url = path.toUri().toURL();
                } catch (MalformedURLException e) {
                    return false;
                }
                BufferedInputStream in = new BufferedInputStream(url.openStream());
                ByteBuffer buffer = ByteBuffer.allocate(in.available());
                in.read(buffer.array());
                value.set(buffer.array(), 0, buffer.array().length);
                processed = true;
                return true;
            }
            return false;
        }

        public Path getCurrentKey() throws IOException, InterruptedException {
            return path;
        }

        public BytesWritable getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        public float getProgress() throws IOException, InterruptedException {
            return processed ? 1.0f : 0.0f;
        }

        public void close() throws IOException {
            // do nothing
        }
    }
}