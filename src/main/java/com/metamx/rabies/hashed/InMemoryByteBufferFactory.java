package com.metamx.druid.index.hashed;

import java.nio.ByteBuffer;

/**
 */
public class InMemoryByteBufferFactory implements ByteBufferFactory
{
  @Override
  public ByteBuffer create(int size)
  {
    return ByteBuffer.allocate(size);
  }
}
