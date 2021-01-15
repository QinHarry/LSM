package hao.tool.lsm;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;

public class CompactionHashIdxKvDb implements KvDb {

    private final static int MAX_LOG_SIZE_BYTE = 100;
    private final static int LEVEL0_MAX_LOG = 5;
    private final static int LEVEL1_MAX_LOG = 5;
    private final static String PREFIX = "/tmp/test";
    private final static String SUFFIX = ".txt";

    private LogFile curLog;
    private Deque<LogFile> toCompact;
    private Deque<LogFile> compactedLevel1;  // toCompact -> level 1 compact -> compactedLevel1
    private Deque<LogFile> compactedLevel2;  // compactedLevel1 -> level 2 compact -> compactedLevel2
    private MultiHashIdx idx;

    public static void main(String[] args) throws Exception {
        String key = "test";
        String value = "This is a test ";
        CompactionHashIdxKvDb db = new CompactionHashIdxKvDb();
        for (int i = 0; i < 1000; i++) {
            db.set(key + i, value + i);
        }
        System.out.println(db.get(key + 200));
    }

    public CompactionHashIdxKvDb() {
        curLog = LogFile.create(PREFIX + SUFFIX);
        toCompact = new LinkedList<>();
        compactedLevel1 = new LinkedList<>();
        compactedLevel2 = new LinkedList<>();
        idx = new MultiHashIdx();
    }


    @Override
    public void set(String key, String value) {
        if (curLog.size() >= MAX_LOG_SIZE_BYTE) {
            LogFile oldLogFile = curLog;
            oldLogFile.renameTo(PREFIX + "_0_" + toCompact.size() + SUFFIX);
            toCompact.offerLast(oldLogFile);
            curLog = LogFile.create(PREFIX + SUFFIX);
            if (toCompact.size() > LEVEL0_MAX_LOG) {
                new Thread(this::compactLevel1).start();
            }
        }
        String record = Util.composeRecord(key, value);
        long size = curLog.size();
        if (curLog.pathWrite(record) != 0) {
            idx.addIdx(curLog, key, size);
        }
    }

    @Override
    public String get(String key) {
        if (idx.idxOf(curLog, key) != -1) {
            long offset = idx.idxOf(curLog, key);
            return Util.valueOf(curLog.pathRead(offset));
        }
        Map.Entry<LogFile, Long> offsetMap = get(toCompact, key);
        if (offsetMap != null) {
            return Util.valueOf(offsetMap.getKey().pathRead(offsetMap.getValue()));
        }
        offsetMap = get(compactedLevel1, key);
        if (offsetMap != null) {
            return Util.valueOf(offsetMap.getKey().pathRead(offsetMap.getValue()));
        }
        offsetMap = get(compactedLevel2, key);
        if (offsetMap != null) {
            return Util.valueOf(offsetMap.getKey().pathRead(offsetMap.getValue()));
        }
        return null;
    }

    private Map.Entry<LogFile, Long> get(Deque<LogFile> logFileList, String key) {
        for (LogFile lFile : logFileList) {
            if (idx.idxOf(lFile, key) != -1) {
                return new AbstractMap.SimpleEntry<LogFile, Long>(lFile, idx.idxOf(lFile, key));
            }
        }
        return null;
    }

    /**
     * Remove old duplicates in each segment file
     */
    private void compactLevel1() {
        try {
            while (!toCompact.isEmpty()) {
                LogFile newLogFile = LogFile.create(PREFIX + "_1_" + SUFFIX);
                LogFile logFile = toCompact.peekFirst();
                idx.allIdxOf(logFile).forEach((key, offset) -> {
                    String record = logFile.pathRead(offset);
                    long curSize = newLogFile.size();
                    if (newLogFile.pathWrite(record) != 0) {
                        idx.addIdx(newLogFile, key, curSize);
                    }
                });
                compactedLevel1.offerLast(newLogFile);
                toCompact.pollFirst();
                logFile.delete();
                idx.remove(logFile);
            }
            if (compactedLevel1.size() >= LEVEL1_MAX_LOG) {
                compactLevel2();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Remove old duplicates among all segment files in level 1
     */
    private void compactLevel2() {
        try {
            Deque<LogFile> snapshot = new LinkedList<>(compactedLevel1);
            if (snapshot.isEmpty()) {
                return;
            }
            int compactSize = snapshot.size();
            LogFile newLogFile = LogFile.create(PREFIX + "_2_" + SUFFIX);
            while (!snapshot.isEmpty()) {
                LogFile logFile = snapshot.pollLast(); // Start from the latest one, reverse order
                logFile.lines().forEach(record -> {
                    String key = Util.keyOf(record);
                    if (idx.idxOf(newLogFile, key) == -1) {
                        long offset = newLogFile.size();
                        if (newLogFile.pathWrite(record) != 0) {
                            idx.addIdx(newLogFile, key, offset);
                        }
                    }
                });
            }
            compactedLevel2.offerLast(newLogFile);
            while (compactSize > 0) {
                LogFile logFile = compactedLevel1.pollFirst();
                logFile.delete();
                idx.remove(logFile);
                compactSize--;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
