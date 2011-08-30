package com.metamx.druid.index.hashed;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class MMappedByteBufferFactory implements ByteBufferFactory
{
  private final AtomicLong fileCounter = new AtomicLong(0);

  private final String storageDir;

  public MMappedByteBufferFactory(
      String storageDir
  )
  {
    this.storageDir = storageDir;
  }

  @Override
  public ByteBuffer create(int size)
  {
    File valuesFile = new File(storageDir, String.format("base-%d.file", fileCounter.incrementAndGet()));
    valuesFile.getParentFile().mkdirs();
    if (valuesFile.exists()) {
      valuesFile.delete();
    }

    try {
      return new RandomAccessFile(valuesFile, "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, 0, size);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
