package cn.com.wmg.techcarnival;


import cn.com.wmg.techcanival.SelectEngine;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class TestSelectEngine {

    @Test
    public void testSelect() throws Exception{

        File dataFileDir = new File("./data/");
        File resultFile = new File("./result/results");
        SelectEngine selectEngine = new SelectEngine();
        long start = System.currentTimeMillis();
        //1、数据预处理
        selectEngine.preSelect(dataFileDir.getAbsolutePath());

        long middle = System.currentTimeMillis();
        System.out.println("preselect cost " + (System.currentTimeMillis() - start) + " ms");

        //1、回答查询
        try (BufferedReader br = new BufferedReader(new FileReader(resultFile))) {
            String str;
            while ((str = br.readLine()) != null) {
                String[] resultArr = str.split(" ");

                String columnName = resultArr[0];
                Integer k = Integer.parseInt(resultArr[1]);
                String result = resultArr[2];
                Assert.assertEquals(result, selectEngine.bigDataSelect(columnName,k));
            }
        }
        System.out.println("sort cost " + (System.currentTimeMillis() - middle) + " ms");
        System.out.println("total cost " + (System.currentTimeMillis() - start) + " ms");

    }

//    @Test
//    public void calculate() {
//        System.out.println(Integer.MAX_VALUE);
//        System.out.println((1L<<32) - 1);
//        System.out.println((1L << 7));
//        System.out.println(Long.MAX_VALUE);
//        for (int i = 0; i < 63; i++) {
//            if ((Long.MAX_VALUE - (1L << i)) >> 51 < 2048L) {
//                System.out.println(i);
//                System.out.println((1L << i ));
//                break;
//            }
//        }
//    }

}
