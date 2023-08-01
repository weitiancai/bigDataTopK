package cn.com.wmg.techcanival;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class SelectDriver {

    public static void main(String[] args) throws IOException {

        File dataFileDir = new File("/user/syjnh/data/");
        File resultFile = new File("/user/syjnh/result/results");
        SelectEngine selectEngine = new SelectEngine();

        long start = System.currentTimeMillis();
        //1、数据预处理
        selectEngine.preSelect(dataFileDir.getAbsolutePath());

        //1、回答查询
        try (BufferedReader br = new BufferedReader(new FileReader(resultFile))) {
            String str;
            while ((str = br.readLine()) != null) {
                String[] resultArr = str.split(" ");

                String columnName = resultArr[0];
                Integer k = Integer.parseInt(resultArr[1]);
                String result = resultArr[2];
                if (!result.equals(selectEngine.bigDataSelect(columnName, k))) {
                    throw new RuntimeException("program error");
                };
            }
        }
        System.out.println("cost " + (System.currentTimeMillis() - start) + " ms");

    }
}
