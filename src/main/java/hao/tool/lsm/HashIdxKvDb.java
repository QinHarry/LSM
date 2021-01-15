package hao.tool.lsm;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 10, time = 5, timeUnit = TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class HashIdxKvDb implements KvDb {

    private LogFile logFile;
    private Map<String, Long> idx;
    private static int i = 0;
    private static Random random = new Random();


    @Override
    public void set(String key, String value) {
        String record = Util.composeRecord(key, value);
        long size = logFile.size();
        if (logFile.pathWrite(record) != 0) {
            idx.put(key, size);
        }
    }

    @Override
    public String get(String key) {
        if (!idx.containsKey(key)) {
            return "";
        }
        long offset = idx.get(key);
        return Util.valueOf(logFile.pathRead(offset));
    }

    @Benchmark
    public void testSet() {
        String key = "test" + (++i);
        set(key, "This is a test.");
        print(get(key));
    }

    private void print(String a) {
    }

    @Setup
    public void prepare() {
        idx = new HashMap<>();
        logFile = new LogFile("/tmp/test1.txt");
    }

    public static void main(String[] args) throws Exception {
        Options options = new OptionsBuilder()
                .include(HashIdxKvDb.class.getSimpleName())
                .forks(1)
                .output("/tmp/Benchmark.log")
                .build();
        new Runner(options).run();
    }
}
