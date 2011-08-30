package com.metamx.druid.index.hashed;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 */
public class ByteBufferBasedHashTest
{
  private ByteBufferBasedHash<Integer, FloatBuffer> hash;
  private float[][] expectedValues;


  @Before
  public void setUp() throws Exception
  {
    hash = ByteBufferBasedHash.create(
        new ByteBufferBasedHashConfig(100, 10),
        new InMemoryByteBufferFactory(),
        new ByteBufferHashStrategy<Integer, FloatBuffer>()
        {
          @Override
          public void keyIntoBuffer(ByteBuffer buffer, Integer key)
          {
            buffer.putInt(key);
          }

          @Override
          public void valueIntoBuffer(ByteBuffer buffer, FloatBuffer value)
          {
            buffer.asFloatBuffer().put(value);
          }

          @Override
          public Integer keyFromBuffer(ByteBuffer buffer)
          {
            return buffer.getInt();
          }

          @Override
          public FloatBuffer valueFromBuffer(ByteBuffer buffer)
          {
            return buffer.asFloatBuffer();
          }

          @Override
          public int keyCompare(Integer lhs, Integer rhs)
          {
            return lhs.compareTo(rhs);
          }

          @Override
          public int sizeOfKey()
          {
            return 4;
          }

          @Override
          public int sizeOfValue()
          {
            return 4 * 3;
          }
        },
        new HashFunction()
        {
          @Override
          public byte[] hash(byte[] object)
          {
            if (true) {
              return object;
            }

            try {
              return MessageDigest.getInstance("MD5").digest(object);
            }
            catch (NoSuchAlgorithmException e) {
              throw new RuntimeException(e);
            }
          }
        }
    );

    expectedValues = new float[][]{
        new float[]{0.0f, 0.0f, 0.0f},
        new float[]{0.1f, 0.2f, 0.3f},
        new float[]{0.2f, 0.4f, 0.6f},
        new float[]{0.3f, 0.6f, 0.9f},
        new float[]{0.4f, 0.8f, 1.2f},
    };
  }


  @Test
  public void testSanity() throws Exception
  {
    for (int i = 0; i < 5; ++i) {
      hash.put(i, FloatBuffer.wrap(new float[]{i / 10f, (2 * i) / 10f, (3 * i) / 10f}));
    }

    for (int i = 0; i < 5; ++i) {
      assertEquals(expectedValues[i], hash.get(i));
    }
    Assert.assertNull(hash.get(150));
  }

  @Test
  public void testUpdateInPlace() throws Exception
  {
    testSanity();

    FloatBuffer buf = hash.get(1);
    buf.mark();
    float[] vals = new float[3];
    buf.get(vals);
    for (int i = 0; i < vals.length; i++) {
      vals[i] += 1.0f;
    }
    buf.reset();
    buf.put(vals);
    expectedValues[1] = new float[]{1.1f, 1.2f, 1.3f};

    for (int i = 0; i < 5; ++i) {
      assertEquals(expectedValues[i], hash.get(i));
    }
  }

  @Test
  public void testPutOverwritesValue() throws Exception
  {
    testSanity();

    FloatBuffer buf = hash.get(2);
    float[] vals = new float[3];
    buf.get(vals);
    for (int i = 0; i < vals.length; i++) {
      vals[i] += 1.0f;
    }
    hash.put(2, FloatBuffer.wrap(vals));

    expectedValues[2] = new float[]{1.2f, 1.4f, 1.6f};

    for (int i = 0; i < 5; ++i) {
      assertEquals(expectedValues[i], hash.get(i));
    }
  }

  @Test
  public void testCollisionsSupported() throws Exception
  {
    testSanity();

    hash.put(101, FloatBuffer.wrap(expectedValues[0]));

    assertEquals(expectedValues[1], hash.get(1));
    assertEquals(expectedValues[0], hash.get(101));
  }

  private static void assertEquals(float[] expected, FloatBuffer actual)
  {
    float[] actualVal = new float[actual.remaining()];
    actual.get(actualVal);
    Assert.assertArrayEquals(expected, actualVal, 0.0f);
  }
}
