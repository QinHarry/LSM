package hao.tool.lsm;

public interface KvDb {

    void set(String key, String value);
    String get(String key);
}
