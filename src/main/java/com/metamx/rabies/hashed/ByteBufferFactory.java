package com.metamx.druid.index.hashed;

import java.nio.ByteBuffer;

/**
 */
public interface ByteBufferFactory
{
  public ByteBuffer create(int size);
}
