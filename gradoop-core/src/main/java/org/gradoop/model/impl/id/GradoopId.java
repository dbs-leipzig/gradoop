/*
 * This file is part of gradoop.
 *
 * gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.id;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

/**
 * Primary key for an EPGM entity. A GradoopId uniquely identifies an entity
 * inside its domain, i.e. a graph is unique among all graphs, a vertex among
 * all vertices and an edge among all edges.
 *
 * @see org.gradoop.model.api.EPGMIdentifiable
 */
public class GradoopId implements Comparable<GradoopId>,
  WritableComparable<GradoopId>, Serializable {

  /**
   * ID length in Byte
   */
  private static final int ID_LENGTH = 13;

  /**
   * Stores the ID
   *
   * Byte 0 - 1:  Context
   * Byte 1 - 5:  Creator ID
   * Byte 5 - 13: Sequence number
   */
  private byte[] content = new byte[ID_LENGTH];

  /**
   * Empty constructor is necessary for (de-)serialization
   */
  public GradoopId() {
  }

  /**
   * Creates a new Gradoop ID.
   *
   * @param sequence  sequence number
   * @param creatorId creator id
   * @param context   creation context
   */
  GradoopId(long sequence, int creatorId, Context context) {
    Bytes.putByte(content, 0, (byte) context.ordinal());
    Bytes.putInt(content, 1, creatorId);
    Bytes.putLong(content, 5, sequence);
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
    return Arrays.equals(content, that.content);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(content);
  }

  @Override
  public int compareTo(GradoopId o) {
    return Bytes.compareTo(this.content, o.content);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.write(content);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    dataInput.readFully(content);
  }

  @Override
  public String toString() {
    Context context = Context.values()[(int) content[0]];
    int creator = Bytes.toInt(content, 1);
    long sequence = Bytes.toLong(content, 5);
    return String.format("[%s-%s-%s]",
      Long.toHexString(sequence),
      Integer.toHexString(creator),
      context);
  }
}
