/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.common.model.impl.properties.bytes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Comparator;

/**
 * Utility class that handles byte arrays, conversions to/from other types,
 * comparisons, hash code generation, manufacturing keys for HashMaps or HashSets, etc.
 *
 * This implementation reuses much of the code of HBase's Bytes (org.apache.hadoop.hbase.util.Bytes).
 * This can be found in org.apache.hbase:hbase-common
 * Much of the code is copied directly or has only small changes.
 */
public class Bytes implements Comparable<Bytes> {

  /**
   * Size of boolean in bytes
   */
  public static final int SIZEOF_BOOLEAN = Byte.SIZE / Byte.SIZE;

  /**
   * Size of byte in bytes
   */
  public static final int SIZEOF_BYTE = SIZEOF_BOOLEAN;

  /**
   * Size of char in bytes
   */
  public static final int SIZEOF_CHAR = Character.SIZE / Byte.SIZE;

  /**
   * Size of double in bytes
   */
  public static final int SIZEOF_DOUBLE = Double.SIZE / Byte.SIZE;

  /**
   * Size of float in bytes
   */
  public static final int SIZEOF_FLOAT = Float.SIZE / Byte.SIZE;

  /**
   * Size of int in bytes
   */
  public static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;

  /**
   * Size of long in bytes
   */
  public static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;

  /**
   * Size of short in bytes
   */
  public static final int SIZEOF_SHORT = Short.SIZE / Byte.SIZE;

  /**
   * true, if unsafe is available
   */
  private static final boolean UNSAFE_UNALIGNED = UnsafeAvailChecker.unaligned();

  /**
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(Bytes.class);

  /**
   * Byte array
   */
  private byte[] bytes;

  /**
   * offset
   */
  private int offset;

  /**
   * length
   */
  private int length;

  /**
   * Use comparing byte arrays, byte-by-byte
   */
  private final ByteArrayComparator bytesRawcomparator = new ByteArrayComparator();

  /**
   * Create a zero-size sequence.
   */
  public Bytes() {
    super();
  }

  /**
   * Create a Bytes using the byte array as the initial value.
   *
   * @param bytes This array becomes the backing storage for the object.
   */
  public Bytes(byte[] bytes) {
    this(bytes, 0, bytes.length);
  }

  /**
   * Set the new Bytes to the contents of the passed
   * {@code ibw}
   *
   * @param ibw the value to set this Bytes to.
   */
  public Bytes(final Bytes ibw) {
    this(ibw.get(), ibw.getOffset(), ibw.getLength());
  }

  /**
   * Set the value to a given byte range
   *
   * @param bytes the new byte range to set to
   * @param offset the offset in newData to start at
   * @param length the number of bytes in the range
   */
  public Bytes(final byte[] bytes, final int offset,
               final int length) {
    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
  }

  /**
   * Get the data from the Bytes.
   *
   * @return The data is only valid between offset and offset+length.
   */
  public byte[] get() {
    if (this.bytes == null) {
      throw new IllegalStateException("Uninitialiized. Null constructor " +
        "called w/o accompaying readFields invocation");
    }
    return this.bytes;
  }

  /**
   * Set byte array with offset 0 and length of the byte array.
   *
   * @param b Use passed bytes as backing array for this instance.
   */
  public void set(final byte[] b) {
    set(b, 0, b.length);
  }

  /**
   * Set byte array, offset and length.
   *
   * @param b Use passed bytes as backing array for this instance.
   * @param offset offset
   * @param length length
   */
  public void set(final byte[] b, final int offset, final int length) {
    this.bytes = b;
    this.offset = offset;
    this.length = length;
  }

  /**
   * Get length
   *
   * @return the number of valid bytes in the buffer
   */
  public int getLength() {
    if (this.bytes == null) {
      throw new IllegalStateException("Uninitialiized. Null constructor " +
        "called w/o accompaying readFields invocation");
    }
    return this.length;
  }

  /**
   * Get offset
   *
   * @return offset
   */
  public int getOffset() {
    return this.offset;
  }

  @Override
  public int hashCode() {
    return hashCode(bytes, offset, length);
  }

  /**
   * Define the sort order of the Bytes.
   *
   * @param that The other bytes writable
   * @return Positive if left is bigger than right, 0 if they are equal, and
   *         negative if left is smaller than right.
   */
  @Override
  public int compareTo(Bytes that) {
    return bytesRawcomparator.compare(
      this.bytes, this.offset, this.length,
      that.bytes, that.offset, that.length);
  }

  @Override
  public boolean equals(Object that) {
    if (that == this) {
      return true;
    }
    if (that instanceof Bytes) {
      return compareTo((Bytes) that) == 0;
    }
    return false;
  }

  @Override
  public String toString() {
    return toString(bytes, offset, length);
  }

  /**
   * Byte array comparator class.
   */
  public static class ByteArrayComparator implements Comparator<byte []> {
    /**
     * Constructor
     */
    ByteArrayComparator() {
      super();
    }

    @Override
    public int compare(byte [] left, byte [] right) {
      return compareTo(left, right);
    }

    /**
     * Compares two byte arrays with a specified offset and length.
     *
     * @param b1 left byte array
     * @param s1 offset of left byte array
     * @param l1 length of left byte array
     * @param b2 right byte array
     * @param s2 offset of right byte array
     * @param l2 length of right byte array
     * @return 0 if equal, &lt; 0 if left is less than right, etc.
     */
    public int compare(byte [] b1, int s1, int l1, byte [] b2, int s2, int l2) {
      return LexicographicalComparerHolder.BEST_COMPARER.
        compareTo(b1, s1, l1, b2, s2, l2);
    }
  }

  /**
   * Put bytes at the specified byte array position.
   *
   * @param tgtBytes the byte array
   * @param tgtOffset position in the array
   * @param srcBytes array to write out
   * @param srcOffset source offset
   * @param srcLength source length
   * @return incremented offset
   */
  public static int putBytes(byte[] tgtBytes, int tgtOffset, byte[] srcBytes,
                             int srcOffset, int srcLength) {
    System.arraycopy(srcBytes, srcOffset, tgtBytes, tgtOffset, srcLength);
    return tgtOffset + srcLength;
  }

  /**
   * Write a single byte out to the specified byte array position.
   *
   * @param bytes the byte array
   * @param offset position in the array
   * @param b byte to write out
   * @return incremented offset
   */
  public static int putByte(byte[] bytes, int offset, byte b) {
    bytes[offset] = b;
    return offset + 1;
  }

  /**
   * This method will convert utf8 encoded bytes into a string. If the given byte array is null,
   * this method will return null.
   *
   * @param b Presumed UTF-8 encoded byte array.
   * @return String made from <code>b</code>
   */
  public static String toString(final byte [] b) {
    if (b == null) {
      return null;
    }
    return toString(b, 0, b.length);
  }

  /**
   * Joins two byte arrays together using a separator.
   *
   * @param b1 The first byte array.
   * @param sep The separator to use.
   * @param b2 The second byte array.
   * @return input byte arrays as string with a separator between
   */
  public static String toString(final byte [] b1,
                                String sep,
                                final byte [] b2) {
    return toString(b1, 0, b1.length) + sep + toString(b2, 0, b2.length);
  }

  /**
   * This method will convert utf8 encoded bytes into a string. If the given byte array is null,
   * this method will return null.
   *
   * @param b Presumed UTF-8 encoded byte array.
   * @param off offset into array
   * @return String made from <code>b</code> or null
   */
  public static String toString(final byte[] b, int off) {
    if (b == null) {
      return null;
    }
    int len = b.length - off;
    if (len <= 0) {
      return "";
    }
    return new String(b, off, len, StandardCharsets.UTF_8);
  }

  /**
   * This method will convert utf8 encoded bytes into a string. If the given byte array is null,
   * this method will return null.
   *
   * @param b Presumed UTF-8 encoded byte array.
   * @param off offset into array
   * @param len length of utf-8 sequence
   * @return String made from <code>b</code> or null
   */
  public static String toString(final byte [] b, int off, int len) {
    if (b == null) {
      return null;
    }
    if (len == 0) {
      return "";
    }
    return new String(b, off, len, StandardCharsets.UTF_8);
  }

  /**
   * Converts a string to a UTF-8 byte array.
   *
   * @param s string
   * @return the byte array
   */
  public static byte[] toBytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Converts a byte array to a long value.
   *
   * @param bytes array
   * @return the long value
   */
  public static long toLong(byte[] bytes) {
    return toLong(bytes, 0, SIZEOF_LONG);
  }

  /**
   * Converts a byte array to a long value. Assumes there will be {@link #SIZEOF_LONG} bytes available.
   *
   * @param bytes bytes
   * @param offset offset
   * @return the long value
   */
  public static long toLong(byte[] bytes, int offset) {
    return toLong(bytes, offset, SIZEOF_LONG);
  }

  /**
   * Converts a byte array to a long value.
   *
   * @param bytes array of bytes
   * @param offset offset into array
   * @param length length of data (must be {@link #SIZEOF_LONG})
   * @return the long value
   * @throws IllegalArgumentException if length is not {@link #SIZEOF_LONG} or
   * if there's not enough room in the array at the offset indicated.
   */
  public static long toLong(byte[] bytes, int offset, final int length) {
    if (length != SIZEOF_LONG || offset + length > bytes.length) {
      throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_LONG);
    }
    if (UNSAFE_UNALIGNED) {
      return toLongUnsafe(bytes, offset);
    } else {
      long l = 0;
      for (int i = offset; i < offset + length; i++) {
        l <<= 8;
        l ^= bytes[i] & 0xFF;
      }
      return l;
    }
  }

  /**
   * Return Exception with fancy error message.
   *
   * @param bytes used byte array
   * @param offset used offset
   * @param length used length
   * @param expectedLength expected length
   * @return IllegalArgumentException with fancy error message
   */
  private static IllegalArgumentException explainWrongLengthOrOffset(final byte[] bytes,
                                                                     final int offset,
                                                                     final int length,
                                                                     final int expectedLength) {
    String reason;
    if (length != expectedLength) {
      reason = "Wrong length: " + length + ", expected " + expectedLength;
    } else {
      reason = "offset (" + offset + ") + length (" + length + ") exceed the capacity of the array: " +
        bytes.length;
    }
    return new IllegalArgumentException(reason);
  }

  /**
   * Put a long value out to the specified byte array position.
   *
   * @param bytes the byte array
   * @param offset position in the array
   * @param val long to write out
   * @return incremented offset
   * @throws IllegalArgumentException if the byte array given doesn't have
   * enough room at the offset specified.
   */
  public static int putLong(byte[] bytes, int offset, long val) {
    if (bytes.length - offset < SIZEOF_LONG) {
      throw new IllegalArgumentException("Not enough room to put a long at" + " offset " + offset +
        " in a " + bytes.length + " byte array");
    }
    if (UNSAFE_UNALIGNED) {
      return putLongUnsafe(bytes, offset, val);
    } else {
      for (int i = offset + 7; i > offset; i--) {
        bytes[i] = (byte) val;
        val >>>= 8;
      }
      bytes[offset] = (byte) val;
      return offset + SIZEOF_LONG;
    }
  }

  /**
   * Put a long value out to the specified byte array position (Unsafe).
   *
   * @param bytes the byte array
   * @param offset position in the array
   * @param val long to write out
   * @return incremented offset
   */
  private static int putLongUnsafe(byte[] bytes, int offset, long val) {
    if (LexicographicalComparerHolder.UnsafeComparer.LITTLE_ENDIAN) {
      val = Long.reverseBytes(val);
    }
    LexicographicalComparerHolder.UnsafeComparer.UNSAFE.putLong(bytes, (long) offset +
      LexicographicalComparerHolder.UnsafeComparer.BYTE_ARRAY_BASE_OFFSET, val);
    return offset + SIZEOF_LONG;
  }

  /**
   * Presumes float encoded as IEEE 754 floating-point "single format"
   *
   * @param bytes byte array
   * @return Float made from passed byte array.
   */
  public static float toFloat(byte [] bytes) {
    return toFloat(bytes, 0);
  }

  /**
   * Presumes float encoded as IEEE 754 floating-point "single format"
   *
   * @param bytes array to convert
   * @param offset offset into array
   * @return Float made from passed byte array.
   */
  public static float toFloat(byte [] bytes, int offset) {
    return Float.intBitsToFloat(toInt(bytes, offset, SIZEOF_INT));
  }

  /**
   * Put a float value out to the specified byte position.
   *
   * @param bytes byte array
   * @param offset offset to write to
   * @param f float value
   * @return New offset in <code>bytes</code>
   */
  public static int putFloat(byte [] bytes, int offset, float f) {
    return putInt(bytes, offset, Float.floatToRawIntBits(f));
  }

  /**
   * Converts a byte array to a double value.
   *
   * @param bytes byte array
   * @return Return double made from passed bytes.
   */
  public static double toDouble(final byte [] bytes) {
    return toDouble(bytes, 0);
  }

  /**
   * Converts a byte array to a double value.
   *
   * @param bytes byte array
   * @param offset offset where double is
   * @return Return double made from passed bytes.
   */
  public static double toDouble(final byte [] bytes, final int offset) {
    return Double.longBitsToDouble(toLong(bytes, offset, SIZEOF_LONG));
  }

  /**
   * Put a double value out to the specified byte position.
   *
   * @param bytes byte array
   * @param offset offset to write to
   * @param d value
   * @return New offset into array <code>bytes</code>
   */
  public static int putDouble(byte [] bytes, int offset, double d) {
    return putLong(bytes, offset, Double.doubleToLongBits(d));
  }

  /**
   * Converts a byte array to an int value
   *
   * @param bytes byte array
   * @return the int value
   */
  public static int toInt(byte[] bytes) {
    return toInt(bytes, 0, SIZEOF_INT);
  }

  /**
   * Converts a byte array to an int value
   *
   * @param bytes byte array
   * @param offset offset into array
   * @return the int value
   */
  public static int toInt(byte[] bytes, int offset) {
    return toInt(bytes, offset, SIZEOF_INT);
  }

  /**
   * Converts a byte array to an int value
   *
   * @param bytes byte array
   * @param offset offset into array
   * @param length length of int (has to be {@link #SIZEOF_INT})
   * @return the int value
   * @throws IllegalArgumentException if length is not {@link #SIZEOF_INT} or
   * if there's not enough room in the array at the offset indicated.
   */
  public static int toInt(byte[] bytes, int offset, final int length) {
    if (length != SIZEOF_INT || offset + length > bytes.length) {
      throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_INT);
    }
    if (UNSAFE_UNALIGNED) {
      return toIntUnsafe(bytes, offset);
    } else {
      int n = 0;
      for (int i = offset; i < (offset + length); i++) {
        n <<= 8;
        n ^= bytes[i] & 0xFF;
      }
      return n;
    }
  }

  /**
   * Converts a byte array to an int value (Unsafe version)
   *
   * @param bytes byte array
   * @param offset offset into array
   * @return the int value
   */
  public static int toIntUnsafe(byte[] bytes, int offset) {
    if (LexicographicalComparerHolder.UnsafeComparer.LITTLE_ENDIAN) {
      return Integer.reverseBytes(LexicographicalComparerHolder.UnsafeComparer.UNSAFE.getInt(bytes,
        (long) offset + LexicographicalComparerHolder.UnsafeComparer.BYTE_ARRAY_BASE_OFFSET));
    } else {
      return LexicographicalComparerHolder.UnsafeComparer.UNSAFE.getInt(bytes,
        (long) offset + LexicographicalComparerHolder.UnsafeComparer.BYTE_ARRAY_BASE_OFFSET);
    }
  }

  /**
   * Converts a byte array to an short value (Unsafe version)
   *
   * @param bytes byte array
   * @param offset offset into array
   * @return the short value
   */
  private static short toShortUnsafe(byte[] bytes, int offset) {
    if (LexicographicalComparerHolder.UnsafeComparer.LITTLE_ENDIAN) {
      return Short.reverseBytes(LexicographicalComparerHolder.UnsafeComparer.UNSAFE.getShort(bytes,
        (long) offset + LexicographicalComparerHolder.UnsafeComparer.BYTE_ARRAY_BASE_OFFSET));
    } else {
      return LexicographicalComparerHolder.UnsafeComparer.UNSAFE.getShort(bytes,
        (long) offset + LexicographicalComparerHolder.UnsafeComparer.BYTE_ARRAY_BASE_OFFSET);
    }
  }

  /**
   * Converts a byte array to an long value (Unsafe version)
   *
   * @param bytes byte array
   * @param offset offset into array
   * @return the long value
   */
  private static long toLongUnsafe(byte[] bytes, int offset) {
    if (LexicographicalComparerHolder.UnsafeComparer.LITTLE_ENDIAN) {
      return Long.reverseBytes(LexicographicalComparerHolder.UnsafeComparer.UNSAFE.getLong(bytes,
        (long) offset + LexicographicalComparerHolder.UnsafeComparer.BYTE_ARRAY_BASE_OFFSET));
    } else {
      return LexicographicalComparerHolder.UnsafeComparer.UNSAFE.getLong(bytes,
        (long) offset + LexicographicalComparerHolder.UnsafeComparer.BYTE_ARRAY_BASE_OFFSET);
    }
  }

  /**
   * Put an int value out to the specified byte array position.
   *
   * @param bytes the byte array
   * @param offset position in the array
   * @param val int to write out
   * @return incremented offset
   * @throws IllegalArgumentException if the byte array given doesn't have enough room at the
   * offset specified.
   */
  public static int putInt(byte[] bytes, int offset, int val) {
    if (bytes.length - offset < SIZEOF_INT) {
      throw new IllegalArgumentException("Not enough room to put an int at" + " offset " + offset +
        " in a " + bytes.length + " byte array");
    }
    if (UNSAFE_UNALIGNED) {
      return putIntUnsafe(bytes, offset, val);
    } else {
      for (int i = offset + 3; i > offset; i--) {
        bytes[i] = (byte) val;
        val >>>= 8;
      }
      bytes[offset] = (byte) val;
      return offset + SIZEOF_INT;
    }
  }

  /**
   * Put an int value out to the specified byte array position (Unsafe).
   *
   * @param bytes the byte array
   * @param offset position in the array
   * @param val int to write out
   * @return incremented offset
   */
  private static int putIntUnsafe(byte[] bytes, int offset, int val) {
    if (LexicographicalComparerHolder.UnsafeComparer.LITTLE_ENDIAN) {
      val = Integer.reverseBytes(val);
    }
    LexicographicalComparerHolder.UnsafeComparer.UNSAFE.putInt(bytes, (long) offset +
      LexicographicalComparerHolder.UnsafeComparer.BYTE_ARRAY_BASE_OFFSET, val);
    return offset + SIZEOF_INT;
  }

  /**
   * Converts a byte array to a short value
   *
   * @param bytes byte array
   * @return the short value
   */
  public static short toShort(byte[] bytes) {
    return toShort(bytes, 0, SIZEOF_SHORT);
  }

  /**
   * Converts a byte array to a short value
   *
   * @param bytes byte array
   * @param offset offset into array
   * @return the short value
   */
  public static short toShort(byte[] bytes, int offset) {
    return toShort(bytes, offset, SIZEOF_SHORT);
  }

  /**
   * Converts a byte array to a short value
   *
   * @param bytes byte array
   * @param offset offset into array
   * @param length length, has to be {@link #SIZEOF_SHORT}
   * @return the short value
   * @throws IllegalArgumentException if length is not {@link #SIZEOF_SHORT}
   * or if there's not enough room in the array at the offset indicated.
   */
  public static short toShort(byte[] bytes, int offset, final int length) {
    if (length != SIZEOF_SHORT || offset + length > bytes.length) {
      throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_SHORT);
    }
    if (UNSAFE_UNALIGNED) {
      return toShortUnsafe(bytes, offset);
    } else {
      short n = 0;
      n = (short) ((n ^ bytes[offset]) & 0xFF);
      n = (short) (n << 8);
      n = (short) ((n ^ bytes[offset + 1]) & 0xFF);
      return n;
    }
  }

  /**
   * Put a short value out to the specified byte array position.
   *
   * @param bytes the byte array
   * @param offset position in the array
   * @param val short to write out
   * @return incremented offset
   * @throws IllegalArgumentException if the byte array given doesn't have enough room at the
   * offset specified.
   */
  public static int putShort(byte[] bytes, int offset, short val) {
    if (bytes.length - offset < SIZEOF_SHORT) {
      throw new IllegalArgumentException("Not enough room to put a short at" + " offset " + offset +
        " in a " + bytes.length + " byte array");
    }
    if (UNSAFE_UNALIGNED) {
      return putShortUnsafe(bytes, offset, val);
    } else {
      bytes[offset + 1] = (byte) val;
      val >>= 8;
      bytes[offset] = (byte) val;
      return offset + SIZEOF_SHORT;
    }
  }

  /**
   * Put a short value out to the specified byte array position (Unsafe).
   *
   * @param bytes the byte array
   * @param offset position in the array
   * @param val short to write out
   * @return incremented offset
   */
  private static int putShortUnsafe(byte[] bytes, int offset, short val) {
    if (LexicographicalComparerHolder.UnsafeComparer.LITTLE_ENDIAN) {
      val = Short.reverseBytes(val);
    }
    LexicographicalComparerHolder.UnsafeComparer.UNSAFE.putShort(bytes, (long) offset +
      LexicographicalComparerHolder.UnsafeComparer.BYTE_ARRAY_BASE_OFFSET, val);
    return offset + SIZEOF_SHORT;
  }

  /**
   * Convert a BigDecimal value to a byte array
   *
   * @param val input value
   * @return the byte array
   */
  public static byte[] toBytes(BigDecimal val) {
    byte[] valueBytes = val.unscaledValue().toByteArray();
    byte[] result = new byte[valueBytes.length + SIZEOF_INT];
    int offset = putInt(result, 0, val.scale());
    putBytes(result, offset, valueBytes, 0, valueBytes.length);
    return result;
  }


  /**
   * Converts a byte array to a BigDecimal
   *
   * @param bytes bytes
   * @return the char value
   */
  public static BigDecimal toBigDecimal(byte[] bytes) {
    return toBigDecimal(bytes, 0, bytes.length);
  }

  /**
   * Converts a byte array to a BigDecimal value
   *
   * @param bytes bytes
   * @param offset offset
   * @param length length
   * @return the char value
   */
  public static BigDecimal toBigDecimal(byte[] bytes, int offset, final int length) {
    if (bytes == null || length < SIZEOF_INT + 1 ||
      (offset + length > bytes.length)) {
      return null;
    }

    int scale = toInt(bytes, offset);
    byte[] tcBytes = new byte[length - SIZEOF_INT];
    System.arraycopy(bytes, offset + SIZEOF_INT, tcBytes, 0, length - SIZEOF_INT);
    return new BigDecimal(new BigInteger(tcBytes), scale);
  }

  /**
   * Compares two byte arrays
   *
   * @param left left operand
   * @param right right operand
   * @return 0 if equal, &lt; 0 if left is less than right, etc.
   */
  public static int compareTo(final byte [] left, final byte [] right) {
    return LexicographicalComparerHolder.BEST_COMPARER.
      compareTo(left, 0, left.length, right, 0, right.length);
  }

  /**
   * Lexicographically compare two arrays.
   *
   * @param buffer1 left operand
   * @param buffer2 right operand
   * @param offset1 Where to start comparing in the left buffer
   * @param offset2 Where to start comparing in the right buffer
   * @param length1 How much to compare from the left buffer
   * @param length2 How much to compare from the right buffer
   * @return 0 if equal, &lt; 0 if left is less than right, etc.
   */
  public static int compareTo(byte[] buffer1, int offset1, int length1,
                              byte[] buffer2, int offset2, int length2) {
    return LexicographicalComparerHolder.BEST_COMPARER.
      compareTo(buffer1, offset1, length1, buffer2, offset2, length2);
  }

  /**
   * Comparer interface
   *
   * @param <T> type to compare
   */
  interface Comparer<T> {
    /**
     * Compares two buffers of type {@code T} with a given offset and length.
     *
     * @param buffer1 left operand
     * @param offset1 offset of left operand
     * @param length1 length of left operand
     * @param buffer2 left operand
     * @param offset2 offset of right operand
     * @param length2 length of right operand
     * @return 0 if equal, < 0 if left is less than right, etc.
     */
    int compareTo(
      T buffer1, int offset1, int length1, T buffer2, int offset2, int length2
    );
  }

  /**
   * Returns the pure java comparer
   *
   * @return java comparer
   */
  private static Comparer<byte[]> lexicographicalComparerJavaImpl() {
    return LexicographicalComparerHolder.PureJavaComparer.INSTANCE;
  }

  /**
   * Provides a lexicographical comparer implementation; either a Java
   * implementation or a faster implementation based on {@link Unsafe}.
   * <p>
   * Uses reflection to gracefully fall back to the Java implementation if {@code Unsafe} isn't available.
   */
  static class LexicographicalComparerHolder {
    /**
     * Class name of the unsafe comparer
     */
    static final String UNSAFE_COMPARER_NAME =
      LexicographicalComparerHolder.class.getName() + "$UnsafeComparer";

    /**
     * The best comparer
     */
    static final Comparer<byte[]> BEST_COMPARER = getBestComparer();
    /**
     * Returns the Unsafe-using Comparer, or falls back to the pure-Java implementation if unable to do so.
     *
     * @return Unsafe comparer if available, else a pure java comparer
     */
    static Comparer<byte[]> getBestComparer() {
      try {
        Class<?> theClass = Class.forName(UNSAFE_COMPARER_NAME);

        // yes, UnsafeComparer does implement Comparer<byte[]>
        @SuppressWarnings("unchecked")
        Comparer<byte[]> comparer =
          (Comparer<byte[]>) theClass.getEnumConstants()[0];
        return comparer;
      } catch (ClassNotFoundException t) {
        return lexicographicalComparerJavaImpl();
      }
    }

    /**
     * Java comparer (without unsafe)
     */
    enum PureJavaComparer implements Comparer<byte[]> {
      /**
       * Instance
       */
      INSTANCE;

      @Override
      public int compareTo(byte[] buffer1, int offset1, int length1,
                           byte[] buffer2, int offset2, int length2) {
        // Short circuit equal case
        if (buffer1 == buffer2 &&
          offset1 == offset2 &&
          length1 == length2) {
          return 0;
        }
        // Bring WritableComparator code local
        int end1 = offset1 + length1;
        int end2 = offset2 + length2;
        for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
          int a = buffer1[i] & 0xff;
          int b = buffer2[j] & 0xff;
          if (a != b) {
            return a - b;
          }
        }
        return length1 - length2;
      }
    }

    /**
     * Unsafe comparer
     */
    enum UnsafeComparer implements Comparer<byte[]> {
      /**
       * Instance
       */
      INSTANCE;

      /**
       * Unsafe
       */
      static final Unsafe UNSAFE;

      /** The offset to the first element in a byte array. */
      static final int BYTE_ARRAY_BASE_OFFSET;

      static {
        if (UNSAFE_UNALIGNED) {
          UNSAFE = (Unsafe) AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
            try {
              Field f = Unsafe.class.getDeclaredField("theUnsafe");
              f.setAccessible(true);
              return f.get(null);
            } catch (ReflectiveOperationException e) {
              LOG.warn("sun.misc.Unsafe is not accessible");
              return null;
            }
          });
        } else {
          // It doesn't matter what we throw;
          // it's swallowed in getBestComparer().
          throw new Error();
        }

        BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

        // sanity check - this should never fail
        if (UNSAFE.arrayIndexScale(byte[].class) != 1) {
          throw new AssertionError();
        }
      }

      /**
       * true, if byte order is little endian
       */
      static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);

      /**
       * Lexicographically compare two arrays.
       *
       * @param buffer1 left operand
       * @param buffer2 right operand
       * @param offset1 Where to start comparing in the left buffer
       * @param offset2 Where to start comparing in the right buffer
       * @param length1 How much to compare from the left buffer
       * @param length2 How much to compare from the right buffer
       * @return 0 if equal, < 0 if left is less than right, etc.
       */
      @Override
      public int compareTo(byte[] buffer1, int offset1, int length1,
                           byte[] buffer2, int offset2, int length2) {

        // Short circuit equal case
        if (buffer1 == buffer2 &&
          offset1 == offset2 &&
          length1 == length2) {
          return 0;
        }
        final int stride = 8;
        final int minLength = Math.min(length1, length2);
        int strideLimit = minLength & -stride;
        final long offset1Adj = (long) offset1 + BYTE_ARRAY_BASE_OFFSET;
        final long offset2Adj = (long) offset2 + BYTE_ARRAY_BASE_OFFSET;
        int i;

        /*
         * Compare 8 bytes at a time. Benchmarking on x86 shows a stride of 8 bytes is no slower
         * than 4 bytes even on 32-bit. On the other hand, it is substantially faster on 64-bit.
         */
        for (i = 0; i < strideLimit; i += stride) {
          long lw = UNSAFE.getLong(buffer1, offset1Adj + (long) i);
          long rw = UNSAFE.getLong(buffer2, offset2Adj + (long) i);
          if (lw != rw) {
            if (!LITTLE_ENDIAN) {
              return ((lw + Long.MIN_VALUE) < (rw + Long.MIN_VALUE)) ? -1 : 1;
            }

            /*
             * We want to compare only the first index where left[index] != right[index]. This
             * corresponds to the least significant nonzero byte in lw ^ rw, since lw and rw are
             * little-endian. Long.numberOfTrailingZeros(diff) tells us the least significant
             * nonzero bit, and zeroing out the first three bits of L.nTZ gives us the shift to get
             * that least significant nonzero byte. This comparison logic is based on UnsignedBytes
             * comparator from guava v21
             */
            int n = Long.numberOfTrailingZeros(lw ^ rw) & ~0x7;
            return ((int) ((lw >>> n) & 0xFF)) - ((int) ((rw >>> n) & 0xFF));
          }
        }

        // The epilogue to cover the last (minLength % stride) elements.
        for (; i < minLength; i++) {
          int a = buffer1[offset1 + i] & 0xFF;
          int b = buffer2[offset2 + i] & 0xFF;
          if (a != b) {
            return a - b;
          }
        }
        return length1 - length2;
      }
    }
  }

  /**
   * Hashes a specified byte array with offset and length.
   *
   * @param bytes array to hash
   * @param offset offset to start from
   * @param length length to hash
   * @return hash
   */
  public static int hashCode(byte[] bytes, int offset, int length) {
    int hash = 1;
    for (int i = offset; i < offset + length; i++) {
      hash = (31 * hash) + (int) bytes[i];
    }
    return hash;
  }
}
