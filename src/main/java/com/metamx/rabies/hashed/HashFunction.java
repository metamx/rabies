package com.metamx.druid.index.hashed;

/**
 */
public interface HashFunction
{
  public byte[] hash(byte[] object);
}
