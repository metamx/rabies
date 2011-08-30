package com.metamx.druid.index.hashed;

import com.google.common.collect.Lists;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.Pair;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;

/**
 */
public class ByteBufferBasedHash<K, V>
{
  private static final byte NOTHING = 0x0;
  private static final byte SOMETHING = 0x1;
  private static final byte ANOTHER_SLAB = 0x2;

  private final ByteBufferFactory factory;
  private final ByteBufferHashStrategy<K, V> strategy;
  private final HashFunction fn;
  private final List<ByteBuffer> buffers;

  private final BigInteger numHashBuckets;
  private final int onDiskValueSize;
  private final int slabSize;
  private final int slabsPerBuffer;

  private volatile int slabsCounter;

  public static <K, V> ByteBufferBasedHash<K, V> create(
      ByteBufferBasedHashConfig config,
      ByteBufferFactory factory,
      ByteBufferHashStrategy<K, V> strategy,
      HashFunction fn
  )
  {
    return new ByteBufferBasedHash<K, V>(config, factory, strategy, fn);
  }

  private static <K, V> int computeOnDiskValueSize(ByteBufferHashStrategy<K, V> strategy)
  {
    return (Math.max(strategy.sizeOfKey() + strategy.sizeOfValue(), 4) + 1);
  }

  private ByteBufferBasedHash(
      ByteBufferBasedHashConfig config,
      ByteBufferFactory factory,
      ByteBufferHashStrategy<K, V> strategy,
      HashFunction fn
  )
  {
    this.factory = factory;
    this.strategy = strategy;
    this.fn = fn;

    slabSize = config.getNumHashBuckets() * computeOnDiskValueSize(strategy);
    slabsPerBuffer = config.getTargetFileSize() / slabSize;
    if (slabsPerBuffer <= 0) {
      throw new IAE("Target file size[%,d] too small, need at least %,d", config.getTargetFileSize(), slabSize);
    }

    this.buffers = Lists.newArrayList();
    buffers.add(factory.create(slabsPerBuffer * slabSize));
    slabsCounter = 1;

    this.numHashBuckets = BigInteger.valueOf(config.getNumHashBuckets());
    this.onDiskValueSize = computeOnDiskValueSize(strategy);
  }

  public V get(K key)
  {
    final ByteBuffer keyBuffer = ByteBuffer.allocate(strategy.sizeOfKey());
    strategy.keyIntoBuffer(keyBuffer, key);

    byte[] keyBytes = keyBuffer.array();
    int bufferIndex = 0;
    int position = 0;

    while (true) {
      final ByteBuffer slab = buffers.get(bufferIndex).duplicate();
      slab.position(position);

      Pair<byte[], ByteBuffer> pair = getObjectBufferFromSlab(keyBytes, slab);
      ByteBuffer valueBuffer = pair.rhs;

      byte valueByte = valueBuffer.get();

      switch (valueByte) {
        case SOMETHING:
          if (strategy.keyCompare(key, strategy.keyFromBuffer(valueBuffer)) == 0) {
            return strategy.valueFromBuffer(valueBuffer);
          }
          return null;
        case ANOTHER_SLAB:
          int slabNum = valueBuffer.getInt();
          if (slabNum >= slabsCounter) {
            throw new ISE(
                "Corrupt slabs, slabNum[%,d] at key[%s] greater than total slabs[%,d]", slabNum, key, slabsCounter
            );
          }

          bufferIndex = slabNum / slabsPerBuffer;
          position = (slabNum % slabsPerBuffer) * slabSize;

          final byte[] hashBytes = pair.lhs;
          ByteBuffer keyBuf = ByteBuffer.allocate(keyBytes.length + hashBytes.length);
          keyBuf.put(keyBytes).put(hashBytes);

          keyBytes = keyBuf.array();

          continue;
        default:
          return null;
      }
    }
  }

  public void put(K key, V value)
  {
    final ByteBuffer keyBuffer = ByteBuffer.allocate(strategy.sizeOfKey());
    strategy.keyIntoBuffer(keyBuffer, key);

    byte[] keyBytes = keyBuffer.array();
    int bufferIndex = 0;
    int position = 0;

    while (true) {
      final ByteBuffer slab = buffers.get(bufferIndex).duplicate();
      slab.position(position);

      Pair<byte[], ByteBuffer> pair = getObjectBufferFromSlab(keyBytes, slab);
      ByteBuffer valueBuffer = pair.rhs;

      valueBuffer.mark();
      byte valueByte = valueBuffer.get();

      switch (valueByte) {
        case NOTHING:
          valueBuffer.reset();
          valueBuffer.put((byte) 0x1);
          strategy.keyIntoBuffer(valueBuffer, key);
          strategy.valueIntoBuffer(valueBuffer, value);
          return;
        case SOMETHING:
          K storedKey = strategy.keyFromBuffer(valueBuffer);
          if (strategy.keyCompare(key, storedKey) == 0) {
            strategy.valueIntoBuffer(valueBuffer, value);
            return;
          } else {
            V storedValue = strategy.valueFromBuffer(valueBuffer);
            valueBuffer.reset();
            valueBuffer.put(ANOTHER_SLAB);
            valueBuffer.putInt(slabsCounter);

            bufferIndex = slabsCounter / slabsPerBuffer;
            position = (slabsCounter % slabsPerBuffer) * slabSize;

            if (bufferIndex >= buffers.size()) {
              buffers.add(factory.create(slabsPerBuffer * slabSize));
            }

            // TODO: Store the key that collided and continue on...
            final byte[] hashBytes = pair.lhs;
            ByteBuffer keyBuf = ByteBuffer.allocate(keyBytes.length + hashBytes.length);
            keyBuf.put(keyBytes).put(hashBytes);

            keyBytes = keyBuf.array();

            ++slabsCounter;
            continue;
          }
        default:
          throw new UnsupportedOperationException("Unknown valueSpec " + valueByte);
      }
    }
  }

  private Pair<byte[], ByteBuffer> getObjectBufferFromSlab(final byte[] keyBytes, ByteBuffer slab)
  {
    final byte[] hashBytes = fn.hash(keyBytes);
    BigInteger hashed = new BigInteger(hashBytes);
    int index = hashed.mod(numHashBuckets).intValue();

    ByteBuffer valueBuffer = slab.duplicate();
    valueBuffer.position(valueBuffer.position() + index * onDiskValueSize);
    valueBuffer.limit(valueBuffer.position() + onDiskValueSize);
    return new Pair<byte[], ByteBuffer>(hashBytes, valueBuffer);
  }
}
