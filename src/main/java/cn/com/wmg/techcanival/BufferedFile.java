package cn.com.wmg.techcanival;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * BufferedFile
 *
 * @author fub
 * @date 2022/9/6 10:22
 */
public class BufferedFile {

    private BufferedOutputStream out;

    private int count;

    public BufferedFile(String file, int size) throws IOException {
        this.out = new BufferedOutputStream(new FileOutputStream(file), size);
    }

    public void write(final byte[] data) throws IOException {
        out.write(data, 0 , 12);
        count++;
    }

    public int close() throws IOException {
        out.close();
        return count;
    }
}
