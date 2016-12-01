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
import org.apache.hadoop.io.WritableComparable;
import org.bson.types.ObjectId;
import org.gradoop.common.model.api.entities.EPGMIdentifiable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Primary key for an EPGM entity. A GradoopId uniquely identifies an entity
 * inside its domain, i.e. a graph is unique among all graphs, a vertex among
 * all vertices and an edge among all edges.
 *
 * This implementation uses MongoDBs BSON {@link ObjectId} to guarantee uniqueness.
 *
 * @see EPGMIdentifiable
 */
public class GradoopId implements WritableComparable<GradoopId>, NormalizableKey<GradoopId> {

  /**
   * Represents a null id.
   */
  public static final GradoopId NULL_VALUE =
    new GradoopId(new ObjectId(0, 0, (short) 0, 0).toByteArray());

  /**
   * Highest possible Gradoop Id.
   */
  public static final GradoopId MAX_VALUE =
    new GradoopId(
      new ObjectId(
        Integer.MAX_VALUE,
        16777215,
        Short.MAX_VALUE,
        16777215).toByteArray());

  /**
   * Lowest possible Gradoop Id.
   */
  public static final GradoopId MIN_VALUE =
    new GradoopId(new ObjectId(
      Integer.MIN_VALUE,
      0,
      Short.MIN_VALUE,
      0).toByteArray());

  /**
   * Number of bytes to represent an id internally.
   */
  public static final int ID_SIZE = 12;

  /**
   * Internal representation
   */
  private ObjectId objectId;

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
    this.objectId = objectId;
  }

  /**
   * Creates a GradoopId from a given byte representation
   *
   * @param bytes the GradoopId represented by the byte array
   */
  GradoopId(byte[] bytes) {
    this.objectId = new ObjectId(bytes);
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
    return new GradoopId(new ObjectId(string));
  }

  public static GradoopId fromLegacyString(String string) {
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
    return new GradoopId(bytes);
  }

  /**
   * Returns byte representation of a GradoopId
   *
   * @return Byte representation
   */
  @SuppressWarnings({"EI_EXPOSE_REP"})
  public byte[] getRawBytes() {
    return objectId.toByteArray();
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
    return this.objectId.equals(that.objectId);
  }

  @Override
  public int hashCode() {
    return objectId.hashCode();
  }

  @Override
  public int compareTo(GradoopId o) {
    return this.objectId.compareTo(o.objectId);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.write(objectId.toByteArray());
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    byte[] buffer = new byte[ID_SIZE];
    dataInput.readFully(buffer);
    this.objectId = new ObjectId(buffer);
  }

  @Override
  public String toString() {
    return this.objectId.toString();
  }

  @Override
  public int getMaxNormalizedKeyLen() {
    return ID_SIZE;
  }

  @Override
  public void copyNormalizedKey(MemorySegment target, int offset, int len) {
    target.put(offset, objectId.toByteArray(), 0, len);
  }

  @Override
  public void write(DataOutputView out) throws IOException {
    out.write(objectId.toByteArray());
  }

  @Override
  public void read(DataInputView in) throws IOException {
    byte[] buffer = new byte[ID_SIZE];
    in.readFully(buffer);
    this.objectId = new ObjectId(buffer);
  }
}
