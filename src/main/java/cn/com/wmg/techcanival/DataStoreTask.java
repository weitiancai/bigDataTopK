package cn.com.wmg.techcanival;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.Callable;

/**
 * PreSelectTask
 *
 * @author fub
 * @date 2022/8/23 15:14
 */
public class DataStoreTask implements Callable<int[][]> {

    // 不要修改
    public static final int BUCKET_POWER = 7;

    public static final int BUCKET_CAPACITY = 1 << BUCKET_POWER;

    public static final int DIVISOR_POWER = 63 - BUCKET_POWER;

    private static final String FILE_SEPARATOR = File.separator;

//    public static final String storePath = "/user/syjnh/target";
    public static final String storePath = "target";
    public static final String storePath0 = storePath + FILE_SEPARATOR + 0 + FILE_SEPARATOR;
    public static final String storePath1 = storePath + FILE_SEPARATOR + 1 + FILE_SEPARATOR;
    public static final String storePath2 = storePath + FILE_SEPARATOR + 2 + FILE_SEPARATOR;

    private static final int writeBufferSize = 1024 * 72;

    /**
     * 数据文件
     */
    private final String dataFile;

    /**
     * 数据文件序号
     */
    private final int dataFileNum;

    public DataStoreTask(String dataFile, int dataFileNum) {
        this.dataFile = dataFile;
        this.dataFileNum = dataFileNum;
    }

    @Override
    public int[][] call() throws IOException {
        //long start = System.currentTimeMillis();

        // initialize writer
        BufferedFile[] writersC1 = new BufferedFile[BUCKET_CAPACITY];
        BufferedFile[] writersC2 = new BufferedFile[BUCKET_CAPACITY];
        BufferedFile[] writersC3 = new BufferedFile[BUCKET_CAPACITY];
        for (int i = 0; i < BUCKET_CAPACITY; i++) {
            writersC1[i] = new BufferedFile(storePath0 + i + FILE_SEPARATOR + dataFileNum, writeBufferSize);
            writersC2[i] = new BufferedFile(storePath1 + i + FILE_SEPARATOR + dataFileNum, writeBufferSize);
            writersC3[i] = new BufferedFile(storePath2 + i + FILE_SEPARATOR + dataFileNum, writeBufferSize);
        }
        // initialize counter
        int[] counterC1 = new int[BUCKET_CAPACITY];
        int[] counterC2 = new int[BUCKET_CAPACITY];
        int[] counterC3 = new int[BUCKET_CAPACITY];

        long value = 0L;
        byte[] writeBuffer = new byte[12];

        int readPosition;
        byte b;
        boolean firstFlag = true;

        FileChannel fileChannel = new FileInputStream(dataFile).getChannel();
        MappedByteBuffer mappedByteBuffer = fileChannel.map(MapMode.READ_ONLY, 0, fileChannel.size());

        // 读标题
        while (mappedByteBuffer.hasRemaining()) {
            b = mappedByteBuffer.get();
            if (b == 10) {
                break;
            }
        }
        readPosition = mappedByteBuffer.position();
        writeBuffer[8] = (byte) (readPosition >> 24);
        writeBuffer[9] = (byte) (readPosition >> 16);
        writeBuffer[10] = (byte) (readPosition >> 8);
        writeBuffer[11] = (byte) (readPosition);

        while (mappedByteBuffer.hasRemaining()) {
            b = mappedByteBuffer.get();
            if (b > 47) {// number
                // 左移三位 * 8  左移一位*2  每进一位乘以10
                value = (value << 3) + (value << 1) + (b - 48);
            } else if (b == 44 && firstFlag) {// first comma
                writeBuffer[0] = (byte) (value >> 56);
                writeBuffer[1] = (byte) (value >> 48);
                writeBuffer[2] = (byte) (value >> 40);
                writeBuffer[3] = (byte) (value >> 32);
                writeBuffer[4] = (byte) (value >> 24);
                writeBuffer[5] = (byte) (value >> 16);
                writeBuffer[6] = (byte) (value >> 8);
                writeBuffer[7] = (byte) (value);

                writersC1[writeBuffer[0]].write(writeBuffer);

                value = 0L;
                firstFlag = false;
            } else if (b == 44) {// second comma
                writeBuffer[0] = (byte) (value >> 56);
                writeBuffer[1] = (byte) (value >> 48);
                writeBuffer[2] = (byte) (value >> 40);
                writeBuffer[3] = (byte) (value >> 32);
                writeBuffer[4] = (byte) (value >> 24);
                writeBuffer[5] = (byte) (value >> 16);
                writeBuffer[6] = (byte) (value >> 8);
                writeBuffer[7] = (byte) (value);

                writersC2[writeBuffer[0]].write(writeBuffer);

                value = 0L;
            } else if (b == 10) {// line feed
                writeBuffer[0] = (byte) (value >> 56);
                writeBuffer[1] = (byte) (value >> 48);
                writeBuffer[2] = (byte) (value >> 40);
                writeBuffer[3] = (byte) (value >> 32);
                writeBuffer[4] = (byte) (value >> 24);
                writeBuffer[5] = (byte) (value >> 16);
                writeBuffer[6] = (byte) (value >> 8);
                writeBuffer[7] = (byte) (value);

                writersC3[writeBuffer[0]].write(writeBuffer);

                //记录offset
                readPosition = mappedByteBuffer.position();
                writeBuffer[8] = (byte) (readPosition >> 24);
                writeBuffer[9] = (byte) (readPosition >> 16);
                writeBuffer[10] = (byte) (readPosition >> 8);
                writeBuffer[11] = (byte) (readPosition);

                value = 0L;
                firstFlag = true;
            }
        }

        for (int i = 0; i < BUCKET_CAPACITY; i++) {
            counterC1[i] = writersC1[i].close();
            counterC2[i] = writersC2[i].close();
            counterC3[i] = writersC3[i].close();
        }

        int[][] counterList = new int[3][];
        counterList[0] = counterC1;
        counterList[1] = counterC2;
        counterList[2] = counterC3;

        //System.out.println("file:" + dataFile + " cost:" + (System.currentTimeMillis() - start) + " ms");
        return counterList;
    }
}
