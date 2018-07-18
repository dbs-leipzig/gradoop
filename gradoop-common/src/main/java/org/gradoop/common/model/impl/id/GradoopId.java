/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.common.model.impl.id;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.NormalizableKey;
import org.bson.types.ObjectId;
import org.gradoop.common.model.api.entities.EPGMIdentifiable;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

import java.io.IOException;

/**
 * Primary key for an EPGM element.
 *
 * This implementation uses a BSON {@link ObjectId} to guarantee uniqueness. Performance critical
 * methods, e.g. {@link GradoopId#equals(Object)} and {@link GradoopId#hashCode()} contain code
 * copied from {@link ObjectId} to avoid unnecessary object instantiations.
 *
 * @see EPGMIdentifiable
 */
public class GradoopId implements NormalizableKey<GradoopId>, CopyableValue<GradoopId> {

  /**
   * Represents a null id.
   */
  public static final GradoopId NULL_VALUE =
    new GradoopId(new ObjectId(0, 0, (short) 0, 0));

  /**
   * Highest possible Gradoop Id.
   */
  public static final GradoopId MAX_VALUE = new GradoopId(
    new ObjectId(Integer.MAX_VALUE, 16777215, Short.MAX_VALUE, 16777215));

  /**
   * Lowest possible Gradoop Id.
   */
  public static final GradoopId MIN_VALUE = new GradoopId(
    new ObjectId(Integer.MIN_VALUE, 0, Short.MIN_VALUE, 0));

  /**
   * Number of bytes to represent an id internally.
   */
  public static final int ID_SIZE = 12;

  /**
   * Required for {@link GradoopId#toString()}
   */
  private static final char[] HEX_CHARS = new char[] {
      '0', '1', '2', '3', '4', '5', '6', '7',
      '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

  /**
   * Internal byte representation
   */
  private byte[] bytes;

  /**
   * Required default constructor for instantiation by serialization logic.
   */
  public GradoopId() {
    bytes = new byte[ID_SIZE];
  }

  /**
   * Create GradoopId from existing ObjectId.
   *
   * @param objectId ObjectId
   */
  GradoopId(ObjectId objectId) {
    this.bytes = objectId.toByteArray();
  }

  /**
   * Creates a GradoopId from a given byte representation
   *
   * @param bytes the GradoopId represented by the byte array
   */
  private GradoopId(byte[] bytes) {
    this.bytes = bytes;
  }

  /**
   * Returns a new GradoopId
   *
   * @return new GradoopId
   */
  public static GradoopId get() {
    return new GradoopId(new ObjectId());
  }

  /**
   * Returns the Gradoop ID represented by a specified hexadecimal string.
   *
   * Note: Implementation taken from {@link ObjectId#ObjectId(String)} to avoid object
   * instantiation.
   *
   * @param string hexadecimal GradoopId representation
   * @return GradoopId
   */
  public static GradoopId fromString(String string) {
    if (!ObjectId.isValid(string)) {
      throw new IllegalArgumentException(
        "invalid hexadecimal representation of a GradoopId: [" + string + "]");
    }

    byte[] b = new byte[12];
    for (int i = 0; i < b.length; i++) {
      b[i] = (byte) Integer.parseInt(string.substring(i * 2, i * 2 + 2), 16);
    }
    return new GradoopId(b);
  }

  /**
   * Returns the Gradoop ID represented by a byte array
   *
   * @param bytes byte representation
   * @return Gradoop ID
   */
  public static GradoopId fromByteArray(byte[] bytes) {
    return new GradoopId(bytes);
  }

  /**
   * Returns byte representation of a GradoopId
   *
   * @return Byte representation
   */
  @SuppressWarnings(value = "EI_EXPOSE_REP", justification = "never mutated")
  public byte[] toByteArray() {
    return bytes;
  }

  /**
   * Checks if the specified object is equal to the current id.
   *
   * @param o the object to be compared
   * @return true, iff the specified id is equal to this id
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    return equals(bytes, ((GradoopId) o).bytes, 0, 0);
  }

  /**
   * Returns the hash code of this GradoopId.
   *
   * Note: Implementation is taken from {@link ObjectId#hashCode()} to avoid object instantiation.
   *
   * @return hash code
   */
  @Override
  public int hashCode() {
    int result = getTimeStamp();
    result = 31 * result + getMachineIdentifier();
    result = 31 * result + (int) getProcessIdentifier();
    result = 31 * result + getCounter();
    return result;
  }

  /**
   * Performs a byte-wise comparison of this and the specified GradoopId.
   *
   * @param o the object to be compared.
   * @return  a negative integer, zero, or a positive integer as this object
   *          is less than, equal to, or greater than the specified object.
   */
  @Override
  public int compareTo(GradoopId o) {
    return compare(this.bytes, o.bytes);
  }

  /**
   * Returns hex string representation of a GradoopId.
   *
   * Note: Implementation is taken from {@link ObjectId#toString()} to avoid object instantiation.
   *
   * @return GradoopId string representation.
   */
  @Override
  public String toString() {
    char[] chars = new char[24];
    int i = 0;
    for (byte b : bytes) {
      chars[i++] = HEX_CHARS[b >> 4 & 0xF];
      chars[i++] = HEX_CHARS[b & 0xF];
    }
    return new String(chars);
  }

  //------------------------------------------------------------------------------------------------
  // methods inherited from NormalizableKey
  //------------------------------------------------------------------------------------------------

  @Override
  public int getMaxNormalizedKeyLen() {
    return ID_SIZE;
  }

  @Override
  public void copyNormalizedKey(MemorySegment target, int offset, int len) {
    target.put(offset, bytes, 0, len);
  }

  @Override
  public void write(DataOutputView out) throws IOException {
    out.write(bytes);
  }

  @Override
  public void read(DataInputView in) throws IOException {
    in.readFully(bytes);
  }

  //------------------------------------------------------------------------------------------------
  // methods inherited from CopyableValue
  //------------------------------------------------------------------------------------------------

  @Override
  public int getBinaryLength() {
    return ID_SIZE;
  }

  @Override
  public void copyTo(GradoopId target) {
    target.bytes = this.bytes;
  }

  @Override
  public GradoopId copy() {
    return new GradoopId(this.bytes);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    target.write(source, ID_SIZE);
  }

  //------------------------------------------------------------------------------------------------
  // private little helpers
  //------------------------------------------------------------------------------------------------

  /**
   * Returns the timestamp component of the id.
   *
   * @return the timestamp
   */
  private int getTimeStamp() {
    return makeInt(bytes[0], bytes[1], bytes[2], bytes[3]);
  }

  /**
   * Returns the machine identifier component of the id.
   *
   * @return the machine identifier
   */
  private int getMachineIdentifier() {
    return makeInt((byte) 0, bytes[4], bytes[5], bytes[6]);
  }

  /**
   * Returns the process identifier component of the id.
   *
   * @return the process identifier
   */
  private short getProcessIdentifier() {
    return (short) makeInt((byte) 0, (byte) 0, bytes[7], bytes[8]);
  }

  /**
   * Returns the counter component of the id.
   *
   * @return the counter
   */
  private int getCounter() {
    return makeInt((byte) 0, bytes[9], bytes[10], bytes[11]);
  }


  //------------------------------------------------------------------------------------------------
  // static helper functions
  //------------------------------------------------------------------------------------------------

  /**
   * Compares the given GradoopIds and returns the smaller one. It both are equal, the first
   * argument is returned.
   *
   * @param first first GradoopId
   * @param second second GradoopId
   * @return smaller GradoopId or first if equal
   */
  public static GradoopId min(GradoopId first, GradoopId second) {
    int comparison = first.compareTo(second);
    return comparison == 0 ? first : (comparison == -1 ? first : second);
  }

  /**
   * Checks if the Gradoop ids stored at the specified positions are equal.
   *
   * Note: The order in which the id components are compared is taken from
   * {@link ObjectId#equals(Object)}. However, we compare the values of the byte arrays directly.
   *
   * @param first first gradoop id
   * @param second second gradoop id
   * @param firstPos start index in the first byte array
   * @param secondPos start index in the second byte array
   *
   * @return true, iff first is equal to second
   */
  static boolean equals(byte[] first, byte[] second, int firstPos, int secondPos) {
    // compare counter (byte 9 to 11)
    if (!equalsInRange(first, second, firstPos + 9, secondPos + 9, 3)) {
      return false;
    }
    // compare machine identifier (byte 4 to 6)
    if (!equalsInRange(first, second, firstPos + 4, secondPos + 4, 2)) {
      return false;
    }
    // compare process identifier (byte 7 to 8)
    if (!equalsInRange(first, second, firstPos + 7, secondPos + 7, 1)) {
      return false;
    }
    // compare timestamp (byte 0 to 3)
    if (!equalsInRange(first, second, firstPos, secondPos, 3)) {
      return false;
    }

    return true;
  }

  /**
   * Compares the specified GradoopIds based on their byte representation
   * (to avoid object instantiation).
   *
   * Note: Implementation is taken from {@link ObjectId#compareTo(Object)} to avoid object
   * instantiation.
   *
   * @param first first GradoopId
   * @param second second GradoopId
   * @return a negative integer, zero, or a positive integer as this object is less than, equal to,
   *         or greater than the specified object.
   */
  private static int compare(byte[] first, byte[] second) {
    return compare(first, second, 0, 0, GradoopId.ID_SIZE);
  }

  /**
   * Compares multiple GradoopIds represented a byte arrays at the specified ranges.
   *
   * @param first first byte representation of multiple gradoop ids
   * @param second second byte representation of multiple gradoop ids
   * @param firstPos start index in the first array
   * @param secondPos start index in the second array
   * @param length length of the range
   *
   * @return a negative integer, zero, or a positive integer as this object is less than, equal to,
   *         or greater than the specified object.
   */
  private static int compare(byte[] first, byte[] second, int firstPos, int secondPos, int length) {
    for (int i = 0; i < length; i++) {
      if (first[firstPos + i] != second[secondPos + i]) {
        return ((first[firstPos + i] & 0xff) < (second[secondPos + i] & 0xff)) ? -1 : 1;
      }
    }
    return 0;
  }

  /**
   * Checks if the given byte arrays contain equal elements in specified given range.
   *
   * @param first first array
   * @param second second array
   * @param firstPos start index in the first array
   * @param secondPos start index in the second array
   * @param length number of bytes to compare for equality
   *
   * @return true, iff both arrays have equal values in the specified range
   */
  private static boolean equalsInRange(byte[] first, byte[] second, int firstPos, int secondPos,
    int length) {
    int upperBound = firstPos + length;
    while (firstPos < upperBound) {
      if (first[firstPos] != second[secondPos]) {
        return false;
      }
      ++firstPos;
      ++secondPos;
    }
    return true;
  }

  /**
   * Returns a primitive int represented by the given 4 bytes.
   *
   * @param b3 byte 3
   * @param b2 byte 2
   * @param b1 byte 1
   * @param b0 byte 0
   *
   * @return int value
   */
  private static int makeInt(final byte b3, final byte b2, final byte b1, final byte b0) {
    return (b3 << 24) | ((b2 & 0xff) << 16) | ((b1 & 0xff) << 8) | ((b0 & 0xff));
  }
}
