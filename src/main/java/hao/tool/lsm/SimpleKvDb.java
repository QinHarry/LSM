package hao.tool.lsm;

import java.io.File;

public class SimpleKvDb implements KvDb{

    private File logFile = new File("/tmp/logFile");

    @Override
    public void set(String key, String value) {
        String record = Util.composeRecord(key, value);
    }

    @Override
    public String get(String key) {
        return null;
    }
}
