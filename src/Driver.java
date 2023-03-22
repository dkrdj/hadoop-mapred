package ssafy;

import org.apache.hadoop.util.ProgramDriver;

public class Driver {
    public static void main(String[] args) {
        int exitCode = -1;
        ProgramDriver pgd = new ProgramDriver();
        try {
//			pgd.addClass("mp3ToWavJob", Mp3ToWavJob.class,"A map/reduce program that performs mp3ToWavJob.");
            pgd.addClass("wordCount", Wordcount.class, "A map/reduce program that performs wordCount.");
            pgd.driver(args);
            exitCode = 0;
        } catch (Throwable e) {
            e.printStackTrace();
        }

        System.exit(exitCode);
    }
}
