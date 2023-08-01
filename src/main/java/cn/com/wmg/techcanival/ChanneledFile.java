package cn.com.wmg.techcanival;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * ChanneledFile
 *
 * @author fub
 * @date 2022/9/5 19:36
 */
public class ChanneledFile {

    private FileChannel fileChannel;

    private ByteBuffer byteBuffer;

    public ChanneledFile(String file, int writeBufferSize) throws IOException {
        this.fileChannel = new FileOutputStream(file).getChannel();
        this.byteBuffer = ByteBuffer.allocate(writeBufferSize);
    }

    public void write(final byte[] data) throws IOException {
        if (byteBuffer.remaining() < 12) {
            flushBuffer();
        }
        byteBuffer.put(data, 0, 12);
    }

    public void flushBuffer() throws IOException {
        byteBuffer.flip();
        fileChannel.write(byteBuffer);
        byteBuffer.clear();
    }

    public int close() throws IOException {
        flushBuffer();
        int count = ((int) fileChannel.size()) / 12;
        fileChannel.close();
        return count;
    }
}
