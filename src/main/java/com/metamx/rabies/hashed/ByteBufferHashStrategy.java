package com.metamx.druid.index.hashed;

import java.nio.ByteBuffer;

/**
 */
public interface ByteBufferHashStrategy<K, V>
{
  public void keyIntoBuffer(ByteBuffer buffer, K key);
  public void valueIntoBuffer(ByteBuffer buffer, V value);

  public K keyFromBuffer(ByteBuffer buffer);
  public V valueFromBuffer(ByteBuffer buffer);

  public int keyCompare(K lhs, K rhs);

  public int sizeOfKey();
  public int sizeOfValue();
}
