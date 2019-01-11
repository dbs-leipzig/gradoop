/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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

import edu.umd.cs.findbugs.annotations.SuppressWarnings;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.NormalizableKey;
import org.gradoop.common.model.api.entities.EPGMIdentifiable;

import java.io.IOException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Date;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Primary key for an EPGM element.
 * <p>
 * This implementation reuses much of the code of BSON's ObjectId
 * (org.bson.types.ObjectId) to guarantee uniqueness. Much of the code is copied directly or
 * has only small changes.
 *
 * @see EPGMIdentifiable
 * <p>
 * references to: org.bson.types.ObjectId
 */
public class GradoopId implements NormalizableKey<GradoopId>, CopyableValue<GradoopId> {

  /**
   * Number of bytes to represent an id internally.
   */
  public static final int ID_SIZE = 12;

  /**
   * Represents a null id.
   */
  public static final GradoopId NULL_VALUE =
    new GradoopId(0, 0, (short) 0, 0);

  /**
   * Integer containing a unique identifier of the machine
   */
  private static final int MACHINE_IDENTIFIER;

  /**
   * Short containing a unique identifier of the process
   */
  private static final short PROCESS_IDENTIFIER;

  /**
   * Integer containing a counter that is increased whenever a new id is created
   */
  private static final AtomicInteger NEXT_COUNTER = new AtomicInteger(new SecureRandom().nextInt());

  /**
   * Bit mask used to extract the lowest three bytes of four
   */
  private static final int LOW_ORDER_THREE_BYTES = 0x00ffffff;

  /**
   * Bit mask used to extract the highest byte of four
   */
  private static final int HIGH_ORDER_ONE_BYTE = 0xff000000;

  /**
   * Required for {@link GradoopId#toString()}
   */
  private static final char[] HEX_CHARS = new char[] {
    '0', '1', '2', '3', '4', '5', '6', '7',
    '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

  /**
   * Internal byte representation
   */
  private byte[] bytes;

  static {
    MACHINE_IDENTIFIER = createMachineIdentifier();
    PROCESS_IDENTIFIER = createProcessIdentifier();
  }

  /**
   * Required default constructor for instantiation by serialization logic.
   */
  public GradoopId() {
    bytes = new byte[ID_SIZE];
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
   * Creates a GradoopId using the given time, machine identifier, process identifier, and counter.
   * <p>
   * Note: Implementation taken from org.bson.types.ObjectId
   *
   * @param timestamp         the time in seconds
   * @param machineIdentifier the machine identifier
   * @param processIdentifier the process identifier
   * @param counter           the counter
   * @throws IllegalArgumentException if the high order byte of machineIdentifier
   *                                  or counter is not zero
   */
  public GradoopId(final int timestamp, final int machineIdentifier,
    final short processIdentifier, final int counter) {
    this(timestamp, machineIdentifier, processIdentifier, counter, true);
  }


  /**
   * Creates a GradoopId using the given time, machine identifier, process identifier, and counter.
   * <p>
   * Note: Implementation taken from org.bson.types.ObjectId
   *
   * @param timestamp         the time in seconds
   * @param machineIdentifier the machine identifier
   * @param processIdentifier the process identifier
   * @param counter           the counter
   * @param checkCounter      if the constructor should test if the counter is between 0 and
   *                          16777215
   */
  private GradoopId(final int timestamp, final int machineIdentifier, final short processIdentifier,
    final int counter, final boolean checkCounter) {
    if ((machineIdentifier & HIGH_ORDER_ONE_BYTE) != 0) {
      throw new IllegalArgumentException("The machine identifier must be between 0" +
        " and 16777215 (it must fit in three bytes).");
    }
    if (checkCounter && ((counter & HIGH_ORDER_ONE_BYTE) != 0)) {
      throw new IllegalArgumentException("The counter must be between 0" +
        " and 16777215 (it must fit in three bytes).");
    }

    ByteBuffer buffer = ByteBuffer.allocate(12);

    buffer.put((byte) (timestamp >> 24));
    buffer.put((byte) (timestamp >> 16));
    buffer.put((byte) (timestamp >> 8));
    buffer.put((byte) timestamp);

    buffer.put((byte) (machineIdentifier >> 16));
    buffer.put((byte) (machineIdentifier >> 8));
    buffer.put((byte) machineIdentifier);

    buffer.put((byte) (processIdentifier >> 8));
    buffer.put((byte) processIdentifier);

    buffer.put((byte) (counter >> 16));
    buffer.put((byte) (counter >> 8));
    buffer.put((byte) counter);

    this.bytes = buffer.array();
  }

  /**
   * Creates the machine identifier from the network interface.
   * <p>
   * Note: Implementation taken from org.bson.types.ObjectId
   *
   * @return a short representing the process
   */
  private static int createMachineIdentifier() {
    int machinePiece;
    try {
      StringBuilder sb = new StringBuilder();
      Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
      while (e.hasMoreElements()) {
        NetworkInterface ni = e.nextElement();
        sb.append(ni.toString());
        byte[] mac = ni.getHardwareAddress();
        if (mac != null) {
          ByteBuffer bb = ByteBuffer.wrap(mac);
          try {
            sb.append(bb.getChar());
            sb.append(bb.getChar());
            sb.append(bb.getChar());
          } catch (BufferUnderflowException shortHardwareAddressException) {
            // mac with less than 6 bytes. continue
          }
        }
      }
      machinePiece = sb.toString().hashCode();
    } catch (SocketException t) {
      machinePiece = new SecureRandom().nextInt();
    }
    machinePiece = machinePiece & LOW_ORDER_THREE_BYTES;
    return machinePiece;
  }

  /**
   * Creates the process identifier.  This does not have to be unique per class loader because
   * NEXT_COUNTER will provide the uniqueness.
   * <p>
   * Note: Implementation taken from org.bson.types.ObjectId
   *
   * @return a short representing the process
   */
  private static short createProcessIdentifier() {
    short processId;
    String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
    if (processName.contains("@")) {
      processId = (short) Integer.parseInt(processName.substring(0, processName.indexOf('@')));
    } else {
      processId = (short) java.lang.management.ManagementFactory
        .getRuntimeMXBean().getName().hashCode();
    }

    return processId;
  }

  /**
   * Returns a new GradoopId
   *
   * @return new GradoopId
   */
  public static GradoopId get() {
    return new GradoopId(dateToTimestampSeconds(new Date()), MACHINE_IDENTIFIER,
      PROCESS_IDENTIFIER, NEXT_COUNTER.getAndIncrement(), false);
  }

  /**
   * Converts a date into the seconds since unix epoch.
   *
   * @param time a time
   * @return int representing the seconds between unix epoch and the given time
   */
  private static int dateToTimestampSeconds(final Date time) {
    return (int) (time.getTime() / 1000);
  }

  /**
   * Returns the Gradoop ID represented by a specified hexadecimal string.
   * <p>
   * Note: Implementation taken from org.bson.types.ObjectId
   *
   * @param string hexadecimal GradoopId representation
   * @return GradoopId
   */
  public static GradoopId fromString(String string) {
    if (!GradoopId.isValid(string)) {
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
   * Checks if a string can be transformed into a GradoopId.
   * <p>
   * Note: Implementation taken from org.bson.types.ObjectId
   *
   * @param hexString a potential GradoopId as a String.
   * @return whether the string could be an object id
   * @throws IllegalArgumentException if hexString is null
   */
  public static boolean isValid(final String hexString) {
    if (hexString == null) {
      throw new IllegalArgumentException();
    }

    int len = hexString.length();
    if (len != 24) {
      return false;
    }

    for (int i = 0; i < len; i++) {
      char c = hexString.charAt(i);
      if (c >= '0' && c <= '9') {
        continue;
      }
      if (c >= 'a' && c <= 'f') {
        continue;
      }
      if (c >= 'A' && c <= 'F') {
        continue;
      }

      return false;
    }

    return true;
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

    byte[] firstBytes = this.bytes;
    byte[] secondBytes = ((GradoopId) o).bytes;
    for (int i = 0; i < GradoopId.ID_SIZE; i++) {
      if (firstBytes[i] != secondBytes[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns the hash code of this GradoopId.
   * <p>
   * Note: Implementation taken from org.bson.types.ObjectId
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
   * @param other the object to be compared.
   * @return a negative integer, zero, or a positive integer as this object
   * is less than, equal to, or greater than the specified object.
   */
  @Override
  public int compareTo(GradoopId other) {

    for (int i = 0; i < GradoopId.ID_SIZE; i++) {
      if (this.bytes[i] != other.bytes[i]) {
        return ((this.bytes[i] & 0xff) < (other.bytes[i] & 0xff)) ? -1 : 1;
      }
    }
    return 0;
  }

  /**
   * Returns hex string representation of a GradoopId.
   * <p>
   * Note: Implementation taken from org.bson.types.ObjectId
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
    return String.valueOf(chars);
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
   * @param first  first GradoopId
   * @param second second GradoopId
   * @return smaller GradoopId or first if equal
   */
  public static GradoopId min(GradoopId first, GradoopId second) {
    int comparison = first.compareTo(second);
    return comparison == 0 ? first : (comparison < 0 ? first : second);
  }

  /**
   * Returns a primitive int represented by the given 4 bytes.
   *
   * @param b3 byte 3
   * @param b2 byte 2
   * @param b1 byte 1
   * @param b0 byte 0
   * @return int value
   */
  private static int makeInt(final byte b3, final byte b2, final byte b1, final byte b0) {
    return (b3 << 24) | ((b2 & 0xff) << 16) | ((b1 & 0xff) << 8) | ((b0 & 0xff));
  }
}
