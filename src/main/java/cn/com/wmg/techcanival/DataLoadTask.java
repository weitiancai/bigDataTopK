package cn.com.wmg.techcanival;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * DataLoadTask
 *
 * @author fub
 * @date 2022/8/29 9:40
 */
public class DataLoadTask implements Callable<List<List<Data>>> {

    private static final int BUCKET_POWER = 11;

    public static final int BUCKET_CAPACITY = 1 << BUCKET_POWER;

    private static final int DIVISOR_POWER = DataStoreTask.DIVISOR_POWER - BUCKET_POWER;

    private static final String FILE_SEPARATOR = File.separator;

    private final String storeFile;

    private final long rangeBegin;

    public DataLoadTask(String storeFile, long rangeBegin) {
        this.storeFile = storeFile;
        this.rangeBegin = rangeBegin;
    }

    @Override
    public List<List<Data>> call() throws Exception {
        List<List<Data>> bucket = new ArrayList<>(BUCKET_CAPACITY);
        for (int i = 0; i < BUCKET_CAPACITY; i++) {
            bucket.add(new ArrayList<>(50));
        }

        long value;
        int offset;
        int dataFileNum = Integer.parseInt(storeFile.substring(storeFile.lastIndexOf(FILE_SEPARATOR) + 1));
        byte[] readBuffer = new byte[12];
        try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(storeFile))) {
            while (input.read(readBuffer, 0 ,12) != -1) {
                value = (readBuffer[0] & 0xFFL) << 56
                        | (readBuffer[1] & 0xFFL) << 48
                        | (readBuffer[2] & 0xFFL) << 40
                        | (readBuffer[3] & 0xFFL) << 32
                        | (readBuffer[4] & 0xFFL) << 24
                        | (readBuffer[5] & 0xFFL) << 16
                        | (readBuffer[6] & 0xFFL) << 8
                        | (readBuffer[7] & 0xFFL);
                offset = readBuffer[8] << 24
                        | (readBuffer[9] & 0xFF) << 16
                        | (readBuffer[10] & 0xFF) << 8
                        | (readBuffer[11] & 0xFF);
                //都减去桶开头的值 只有了后56位，再去右移45位，只留11位数字 用于分桶2048个中
                bucket.get((int) ((value - rangeBegin) >> DIVISOR_POWER))
                        .add(new Data(value, dataFileNum, offset)
                );
            }
        }

        return bucket;
    }
}
