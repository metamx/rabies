package com.metamx.druid.index.hashed;

/**
 */
public class ByteBufferBasedHashConfig
{
  private final int numHashBuckets;
  private final int targetFileSize;

  public ByteBufferBasedHashConfig(
      int numHashBuckets,
      int targetFileSize
  )
  {
    this.numHashBuckets = numHashBuckets;
    this.targetFileSize = targetFileSize;
  }

  public int getNumHashBuckets()
  {
    return numHashBuckets;
  }

  public int getTargetFileSize()
  {
    return targetFileSize;
  }
}
