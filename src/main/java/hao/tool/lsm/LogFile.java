package hao.tool.lsm;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;

//@BenchmarkMode(Mode.AverageTime)
//@OutputTimeUnit(TimeUnit.MILLISECONDS)
//@State(Scope.Benchmark)
public class LogFile {

    private Path path;
    private File file;

    public enum Mode {
        PATH, FILE
    }

    public LogFile(String fileName, Mode mode) {
        if (mode.equals(Mode.PATH)) {
            path = Paths.get(fileName);
        } else {
            file = new File(fileName);
        }
    }

    public long pathWrite(String content) {
        try {
            content += System.lineSeparator();
            byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
            Files.write(path, bytes, StandardOpenOption.APPEND);
            return bytes.length;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public String pathRead(long offset) {
        try (RandomAccessFile rFile = new RandomAccessFile(path.toFile(), "r")) {
            rFile.seek(offset);
            return rFile.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

    /* This way is too slow
    public long mapWrite(String content, int offset) {
        RandomAccessFile randomAccessTargetFile = null;
        FileChannel targetFileChannel = null;
        MappedByteBuffer map = null;
        try {
            randomAccessTargetFile = new RandomAccessFile(file, "rw");
            targetFileChannel = randomAccessTargetFile.getChannel();
            map = targetFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, (long) 1024 * 1024 * 1024);
            map.position(offset);
            map.put(content.getBytes());
            return map.position();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (map != null) {
                    map.clear();
                }
                if (targetFileChannel != null) {
                    targetFileChannel.close();
                }
                if (randomAccessTargetFile != null) {
                    randomAccessTargetFile.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return 0;
    }

    public String mapRead(long offset) {
        try {
            RandomAccessFile randomAccessTargetFile = new RandomAccessFile(file, "rw");
            FileChannel targetFileChannel = randomAccessTargetFile.getChannel();
            MappedByteBuffer map = targetFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, offset);
            byte[] byteArr = new byte[10 * 1024];
            map.get(byteArr, 0, (int) offset);
            return new String(byteArr);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    } */

//    @Setup
//    public void prepare() {
//        path = Paths.get("/tmp/test1.txt");
//        file = new File("/tmp/test2.txt");
//    }

//    @Benchmark
//    public void testWrite1() {
//        String content = "This is a test.";
//        for (int i = 0; i < 1000; i++) {
//            pathWrite(content);
//        }
//    }
//
//    @Benchmark
//    public void testWrite2() {
//        String content = "This is a test.";
//        long pos = 0;
//        for (int i = 0; i < 1000; i++) {
//            pos = mapWrite(content, (int) pos);
//        }
//    }


    public static void main(String[] args) throws Exception {
        Options options = new OptionsBuilder()
                .include(LogFile.class.getSimpleName())
                .forks(2)
                .warmupIterations(5)
                .measurementIterations(10)
                .output("/tmp/Benchmark.log")
                .build();
        new Runner(options).run();
    }

}
