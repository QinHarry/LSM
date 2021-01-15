package hao.tool.lsm;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MultiHashIdx {
    private Map<LogFile, Map<String, Long>> idxs;

    public MultiHashIdx() {
        idxs = new ConcurrentHashMap<>();
    }

    public long idxOf(LogFile file, String key) {
        if (!idxs.containsKey(file) || !idxs.get(file).containsKey(key)) {
            return -1;
        }
        return idxs.get(file).get(key);
    }

    public Map<String, Long> allIdxOf(LogFile logFile) {
        return idxs.get(logFile);
    }

    public void addIdx(LogFile file, String key, long offset) {
        idxs.putIfAbsent(file, new ConcurrentHashMap<>());
        idxs.get(file).put(key, offset);
    }

    public void remove(LogFile logFile) {
        idxs.remove(logFile);
    }
}
