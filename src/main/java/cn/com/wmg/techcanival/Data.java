package cn.com.wmg.techcanival;

/**
 * CsvData
 *
 * @author fub
 * @date 2022/8/19 17:17
 */
public class Data {

    private final long value;

    private final int dataFileNum;

    private final int offset;

    public Data(long value, int dataFileNum, int offset) {
        this.value = value;
        this.dataFileNum = dataFileNum;
        this.offset = offset;
    }

    public long getValue() {
        return value;
    }

    public int getDataFileNum() {
        return dataFileNum;
    }

    public int getOffset() {
        return offset;
    }
}
