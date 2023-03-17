package ssafy;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;
import javax.sound.sampled.AudioFileFormat.Type;
import javax.sound.sampled.UnsupportedAudioFileException;

public class Mp3ToWavReducer extends Reducer<Text, BytesWritable, Text, Text> {

    public void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {

        // 임시 .mp3 파일 생성
        File mp3File = File.createTempFile("mp3_", ".mp3");
        mp3File.deleteOnExit();

        // 바이트 배열을 임시 .mp3 파일로 저장
        Path path = new Path(key.toString());
        FileSystem fs = FileSystem.get(context.getConfiguration());
        OutputStream outputStream = fs.create(path);
        for(BytesWritable value : values) {
            outputStream.write(value.getBytes(), 0, value.getLength());
        }
        outputStream.close();

        // 임시 .mp3 파일을 읽어와 AudioInputStream으로 변환
        AudioInputStream mp3Stream = null;
        try {
            mp3Stream = AudioSystem.getAudioInputStream(mp3File);
        } catch (UnsupportedAudioFileException e) {
            throw new RuntimeException(e);
        }

        // AudioInputStream에서 AudioFormat과 byte[]을 추출
        AudioFormat mp3Format = mp3Stream.getFormat();
        byte[] mp3Bytes = mp3Stream.readAllBytes();
        mp3Stream.close();

        // AudioFormat 변환: mp3 -> wav
        AudioFormat wavFormat = new AudioFormat(
                AudioFormat.Encoding.PCM_SIGNED,
                mp3Format.getSampleRate(),
                16,
                mp3Format.getChannels(),
                mp3Format.getChannels() * 2,
                mp3Format.getSampleRate(),
                false);

        // mp3Bytes를 wavBytes로 변환
        AudioInputStream wavStream = new AudioInputStream(
                new ByteArrayInputStream(mp3Bytes),
                mp3Format,
                mp3Bytes.length / mp3Format.getFrameSize());
        AudioInputStream convertedStream = AudioSystem.getAudioInputStream(wavFormat, wavStream);
        byte[] wavBytes = convertedStream.readAllBytes();
        convertedStream.close();

        // 임시 .wav 파일 생성
        File wavFile = File.createTempFile("wav_", ".wav");
        wavFile.deleteOnExit();

        // wavBytes를 임시 .wav 파일로 저장
        AudioSystem.write(
                new AudioInputStream(new ByteArrayInputStream(wavBytes), wavFormat, wavBytes.length / wavFormat.getFrameSize()),
                Type.WAVE,
                wavFile);

        // .mp3 파일 삭제
        mp3File.delete();

        // 파이썬 인터프리터 실행
        ProcessBuilder pb = new ProcessBuilder("python", "spleeter_separate.py", wavFile.getAbsolutePath());
        pb.redirectErrorStream(true);
        Process p = pb.start();
        p.waitFor();

        // 분리된 보컬 파일을 HDFS에 저장
        File outDir = new File("output");
        File[] files = outDir.listFiles();
        for(File file : files) {
            Path outFile = new Path(key.toString().replace(".mp3", "") + "_" + file.getName());
            fs.copyFromLocalFile(new Path(file.getAbsolutePath()), outFile);
            context.write(key, new Text("분리된 보컬 파일 저장 완료"));
            // 분리된 보컬 파일 삭제
            file.delete();
        }

        // 임시 .wav 파일 삭제
        wavFile.delete();

        // 임시 .mp3 파일 삭제
        mp3File.delete();
    }
}