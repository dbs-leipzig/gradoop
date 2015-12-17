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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Primary key for an EPGM entity. A GradoopId uniquely identifies an entity
 * inside its domain, i.e. a graph is unique among all graphs, a vertex among
 * all vertices and an edge among all edges.
 *
 * @see org.gradoop.model.api.EPGMIdentifiable
 */
public class GradoopId implements Comparable<GradoopId>,
  WritableComparable<GradoopId>, Writable, Serializable {

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
   * Most significant bits of a 128-bit UUID
   */
  private long mostSigBits;

  /**
   * Least significant bits of a 128-bit UUID
   */
  private long leastSigBits;

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
    this.mostSigBits = uuid.getMostSignificantBits();
    this.leastSigBits = uuid.getLeastSignificantBits();
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
    return mostSigBits == that.mostSigBits &&
      leastSigBits == that.leastSigBits;
  }

  @Override
  public int hashCode() {
    long hilo = mostSigBits ^ leastSigBits;
    return ((int) (hilo >> 32)) ^ (int) hilo;
  }

  @Override
  public int compareTo(GradoopId o) {
    return this.mostSigBits < o.mostSigBits ? -1 :
      this.mostSigBits > o.mostSigBits ? 1 :
        this.leastSigBits < o.leastSigBits ? -1 :
          this.leastSigBits > o.leastSigBits ? 1 :
            0;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(mostSigBits);
    dataOutput.writeLong(leastSigBits);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.mostSigBits = dataInput.readLong();
    this.leastSigBits = dataInput.readLong();
  }

  @Override
  public String toString() {
    return new UUID(mostSigBits, leastSigBits).toString();
  }
}
