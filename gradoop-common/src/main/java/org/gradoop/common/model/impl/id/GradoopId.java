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

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.types.NormalizableKey;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;
import org.gradoop.common.model.api.entities.EPGMIdentifiable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Primary key for an EPGM entity. A GradoopId uniquely identifies an entity
 * inside its domain, i.e. a graph is unique among all graphs, a vertex among
 * all vertices and an edge among all edges.
 *
 * @see EPGMIdentifiable
 */
public class GradoopId
  implements WritableComparable<GradoopId>, NormalizableKey<GradoopId> {

  /**
   * Represents a null id.
   */
  public static final GradoopId NULL_VALUE =
    new GradoopId(new UUID(0L, 0L));

  /**
   * Highest possible Gradoop Id.
   */
  public static final GradoopId MAX_VALUE =
    new GradoopId(new UUID(Long.MAX_VALUE, Long.MAX_VALUE));

  /**
   * Lowest possible Gradoop Id.
   */
  public static final GradoopId MIN_VALUE =
    new GradoopId(new UUID(Long.MIN_VALUE, Long.MIN_VALUE));

  /**
   * Number of bytes to represent an id internally.
   */
  private static final int ID_SIZE = 16;

  /**
   * Internal representation
   */
  private byte[] rawBytes = new byte[ID_SIZE];

  /**
   * Create a new UUID.
   */
  public GradoopId() {
  }

  /**
   * Create GradoopId from existing UUID.
   *
   * @param uuid UUID
   */
  GradoopId(UUID uuid) {
    checkNotNull(uuid, "UUID was null");
    ByteBuffer buffer = ByteBuffer.wrap(rawBytes);
    buffer.putLong(uuid.getMostSignificantBits());
    buffer.putLong(uuid.getLeastSignificantBits());
  }

  /**
   * Returns a new GradoopId
   *
   * @return new GradoopId
   */
  public static GradoopId get() {
    return new GradoopId(UUID.randomUUID());
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
    return new GradoopId(UUID.fromString(string));
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
    ByteBuffer buffer = ByteBuffer.wrap(rawBytes);
    return new UUID(buffer.getLong(), buffer.getLong()).toString();
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
