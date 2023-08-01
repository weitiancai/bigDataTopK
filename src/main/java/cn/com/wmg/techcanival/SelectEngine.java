package cn.com.wmg.techcanival;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SelectEngine {

    private static final int STORE_BUCKET_CAPACITY = DataStoreTask.BUCKET_CAPACITY;

    private static final long STORE_BUCKET_RANGE = 1L << DataStoreTask.DIVISOR_POWER;

    private static final int LOAD_BUCKET_CAPACITY = DataLoadTask.BUCKET_CAPACITY;

    private static final String FILE_SEPARATOR = File.separator;

    private static final String storePath = DataStoreTask.storePath;
    private static final String storePath0 = DataStoreTask.storePath0;
    private static final String storePath1 = DataStoreTask.storePath1;
    private static final String storePath2 = DataStoreTask.storePath2;

    private static final int THREAD_NUM = 5;

    public static final ExecutorService executor = Executors.newFixedThreadPool(THREAD_NUM);

    private int[][] counterList;

    private int selectCount = 0;

    private List<String> dataFileList;

    /**
     *
     * 数据预处理. 如果有新文件产生必须统一写入/user/syjnh/target，该目录会定时清空
     * @param dataFileDir 数据文件了路径,
     *
     */
    public void preSelect(String dataFileDir) throws IOException {
        // mkdir
        for (int i = 0; i < STORE_BUCKET_CAPACITY; i++) {
            forceMkdir(new File(storePath0 + i));
            forceMkdir(new File(storePath1 + i));
            forceMkdir(new File(storePath2 + i));
        }

        // 初始化计数器
        counterList = new int[3][STORE_BUCKET_CAPACITY];
        // 初始化dataFile列表
        dataFileList = new ArrayList<>(50);
        // 查询计数
        selectCount = 0;

        File dir = new File(dataFileDir);
        if (!dir.exists() || !dir.isDirectory()) {
            return;
        }
        File[] files = dir.listFiles();
        if (files == null) {
            return;
        }

        List<Future<int[][]>> futureList = new ArrayList<>(files.length);
        int dataFileNum = 0;
        // mac 会将 DS_Store 文件算进来不过问题不大
        for (File file : files) {
            dataFileList.add(file.getAbsolutePath());
            DataStoreTask dataStoreTask = new DataStoreTask(file.getAbsolutePath(), dataFileNum++);
            Future<int[][]> future = executor.submit(dataStoreTask);
            futureList.add(future);
        }

        for (Future<int[][]> future : futureList) {
            try {
                int[][] result = future.get();
                for (int i = 0; i < STORE_BUCKET_CAPACITY; i++) {
                    counterList[0][i] = counterList[0][i] + result[0][i];
                    counterList[1][i] = counterList[1][i] + result[1][i];
                    counterList[2][i] = counterList[2][i] + result[2][i];
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *
     * @param columnName 指定数据文件的列
     * @param k 返回升序排列排第K的数，1 2 2 3 那么3排名第四
     * @return
     */
    public String bigDataSelect(String columnName, int k) throws IOException {
        int columnIndex;
        if ("C1".equalsIgnoreCase(columnName)) {
            columnIndex = 0;
        } else if ("C2".equalsIgnoreCase(columnName)) {
            columnIndex = 1;
        } else if ("C3".equalsIgnoreCase(columnName)) {
            columnIndex = 2;
        } else {
            throw new IllegalArgumentException("param columnName is illegal");
        }

        // find bucketId
        int[] counter = counterList[columnIndex];
        int bucketId = 0;
        while (k > counter[bucketId]) {
            k = k - counter[bucketId];
            bucketId++;
        }

        // load data
        long rangeBegin = STORE_BUCKET_RANGE * bucketId;
        List<Future<List<List<Data>>>> futureList = new ArrayList<>(dataFileList.size());
        File dir = new File(storePath + FILE_SEPARATOR + columnIndex + FILE_SEPARATOR + bucketId);
        for (File file : dir.listFiles()) {
            DataLoadTask dataLoadTask = new DataLoadTask(file.getAbsolutePath(), rangeBegin);
            Future<List<List<Data>>> future = executor.submit(dataLoadTask);
            futureList.add(future);
        }

        List<List<List<Data>>> loadedBucketData = new ArrayList<>(LOAD_BUCKET_CAPACITY);
        for (int i = 0; i < LOAD_BUCKET_CAPACITY; i++) {
            loadedBucketData.add(new ArrayList<>(dataFileList.size()));
        }
        int[] loadCounter = new int[LOAD_BUCKET_CAPACITY];
        for (Future<List<List<Data>>> future : futureList) {
            try {
                List<List<Data>> result = future.get();
                for (int i = 0; i < LOAD_BUCKET_CAPACITY; i++) {
                    loadedBucketData.get(i).add(result.get(i));
                    loadCounter[i] = loadCounter[i] + result.get(i).size();
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        int secondaryBucketId = 0;
        while (k > loadCounter[secondaryBucketId]) {
            k = k - loadCounter[secondaryBucketId];
            secondaryBucketId++;
        }

        List<Data> dataList = new ArrayList<>(2048);
        for (List<Data> subDataList : loadedBucketData.get(secondaryBucketId)) {
            dataList.addAll(subDataList);
        }
        dataList.sort(Comparator.comparing(Data::getValue));
        Data data = dataList.get(k - 1);

        FileChannel fileChannel = new FileInputStream(dataFileList.get(data.getDataFileNum())).getChannel();
        MappedByteBuffer mappedByteBuffer = fileChannel.map(MapMode.READ_ONLY, 0, fileChannel.size());
        mappedByteBuffer.position(data.getOffset());
        byte b;
        int length = 0;
        byte[] bytes = new byte[60];
        String bigDataSelectResult = null;
        while (mappedByteBuffer.hasRemaining()) {
            b = mappedByteBuffer.get();
            if (b > 13) {
                bytes[length++] = b;
            } else if (b == 10) {
                bigDataSelectResult = new String(bytes, 0, length);
                break;
            }
        }

        //shutdown executor
        selectCount++;
        if (selectCount >= 60) {
            executor.shutdownNow();
        }
        return bigDataSelectResult;
    }

    private static void forceMkdir(File directory) throws IOException {
        String message;
        if (directory.exists()) {
            if (!directory.isDirectory()) {
                message = "File " + directory + " exists and is " + "not a directory. Unable to create directory.";
                throw new IOException(message);
            }
        } else if (!directory.mkdirs() && !directory.isDirectory()) {
            message = "Unable to create directory " + directory;
            throw new IOException(message);
        }

    }
}
