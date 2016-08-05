/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.common.model.impl.id;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.types.NormalizableKey;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;
import org.bson.types.ObjectId;
import org.gradoop.common.model.api.entities.EPGMIdentifiable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Primary key for an EPGM entity. A GradoopId uniquely identifies an entity
 * inside its domain, i.e. a graph is unique among all graphs, a vertex among
 * all vertices and an edge among all edges.
 *
 * This implementation uses MongoDBs BSON ObjectId to guarantee uniqueness.
 *
 * @see EPGMIdentifiable
 */
public class GradoopId implements WritableComparable<GradoopId>, NormalizableKey<GradoopId> {

  /**
   * Represents a null id.
   */
  public static final GradoopId NULL_VALUE =
    new GradoopId(new ObjectId(0, 0, (short) 0, 0));

  /**
   * Highest possible Gradoop Id.
   */
  public static final GradoopId MAX_VALUE =
    new GradoopId(
      new ObjectId(
        Integer.MAX_VALUE,
        16777215,
        Short.MAX_VALUE,
        16777215));

  /**
   * Lowest possible Gradoop Id.
   */
  public static final GradoopId MIN_VALUE =
    new GradoopId(new ObjectId(
      Integer.MIN_VALUE,
      0,
      Short.MIN_VALUE,
      0));

  /**
   * Number of bytes to represent an id internally.
   */
  public static final int ID_SIZE = 12;

  /**
   * Internal representation
   */
  private byte[] rawBytes = new byte[ID_SIZE];

  /**
   * Create a new ObjectId.
   */
  public GradoopId() {
  }

  /**
   * Create GradoopId from existing ObjectId.
   *
   * @param objectId ObjectId
   */
  GradoopId(ObjectId objectId) {
    checkNotNull(objectId, "ObjectId was null");
    ByteBuffer buffer = ByteBuffer.wrap(rawBytes);
    int timestamp = objectId.getTimestamp();
    int machineId = objectId.getMachineIdentifier();
    short processId = objectId.getProcessIdentifier();
    int counter = objectId.getCounter();
    buffer.putInt(timestamp);
    buffer.put((byte) (machineId >> 16));
    buffer.put((byte) (machineId >> 8));
    buffer.put((byte) machineId);
    buffer.putShort(processId);
    buffer.put((byte) (counter >> 16));
    buffer.put((byte) (counter >> 8));
    buffer.put((byte) counter);
  }

  /**
   * Creates a GradoopId from a given byte representation
   *
   * @param bytes the GradoopId represented by the byte array
   */
  private GradoopId(byte[] bytes) {
    this.rawBytes = bytes;
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
   * Returns the Gradoop ID represented by a string.
   *
   * @param string string representation
   * @return Gradoop ID
   */
  public static GradoopId fromString(String string) {
    checkNotNull(string, "ID string was null");
    checkArgument(!string.isEmpty(), "ID string was empty");
    String[] split = string.split("-");
    return new GradoopId(new ObjectId(
      Integer.parseInt(split[0]),
      Integer.parseInt(split[1]),
      Short.parseShort(split[2]),
      Integer.parseInt(split[3])
    ));
  }

  /**
   * Returns the Gradoop ID represented by a byte array
   *
   * @param bytes byte representation
   * @return Gradoop ID
   */
  public static GradoopId fromBytes(byte[] bytes) {
    checkNotNull(bytes, "Byte array was null");
    checkArgument(bytes.length == ID_SIZE, "Byte array has wrong size");
    return new GradoopId(bytes);
  }

  /**
   * Returns byte representation of a GradoopId
   *
   * @return Byte representation
   */
  @SuppressWarnings({"EI_EXPOSE_REP"})
  public byte[] getRawBytes() {
    return rawBytes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GradoopId)) {
      return false;
    }
    GradoopId that = (GradoopId) o;
    return Arrays.equals(rawBytes, that.rawBytes);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(rawBytes);
  }

  @Override
  public int compareTo(GradoopId o) {
    return Bytes.compareTo(rawBytes, o.rawBytes);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.write(rawBytes);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    dataInput.readFully(rawBytes);
  }

  @Override
  public String toString() {
    ByteBuffer optimus = ByteBuffer.wrap(rawBytes);
    int timestamp = optimus.getInt();
    byte[] machineIdBytes = new byte[4];
    machineIdBytes[0] = (byte) 0;
    machineIdBytes[1] = optimus.get();
    machineIdBytes[2] = optimus.get();
    machineIdBytes[3] = optimus.get();
    ByteBuffer prime = ByteBuffer.wrap(machineIdBytes);
    int machineId = prime.getInt();
    short processId = optimus.getShort();
    byte[] counterBytes = new byte[4];
    counterBytes[0] = (byte) 0;
    counterBytes[1] = optimus.get();
    counterBytes[2] = optimus.get();
    counterBytes[3] = optimus.get();
    prime = ByteBuffer.wrap(counterBytes);
    int counter = prime.getInt();
    return String.format("%s-%s-%s-%s",
      timestamp, machineId, processId, counter);
  }

  @Override
  public int getMaxNormalizedKeyLen() {
    return ID_SIZE;
  }

  @Override
  public void copyNormalizedKey(MemorySegment target, int offset, int len) {
    target.put(offset, rawBytes, 0, len);
  }

  @Override
  public void write(DataOutputView out) throws IOException {
    out.write(rawBytes);
  }

  @Override
  public void read(DataInputView in) throws IOException {
    in.readFully(rawBytes);
  }
}
