package hao.tool.lsm;

public class Util {

    public static String composeRecord(String key, String value) {
        return key + "," + value;
    }

    public static boolean matchKey(String key, String line) {
        return key.equals(line.split(",")[0]);
    }
}
