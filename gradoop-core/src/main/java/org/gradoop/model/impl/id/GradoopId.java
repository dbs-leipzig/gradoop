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

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

/**
 * Primary key for an EPGM entity. A GradoopId uniquely identifies an entity
 * across the whole graph.
 *
 * @see org.gradoop.model.api.EPGMIdentifiable
 */
public final class GradoopId implements Comparable<GradoopId>,
  WritableComparable<GradoopId>, Serializable {

  /**
   * maximum id
   */
  public static final GradoopId MAX_VALUE = new GradoopId(Long.MAX_VALUE);

  /**
   * Serial version uid.
   */
  private static final long serialVersionUID = 42L;

  /**
   * Long identifier.
   */
  private Long identifier;

  /**
   * Default constructor for serialization.
   */
  public GradoopId() {
  }

  /**
   * Initializes a GradoopId from the given parameter.
   *
   * @param identifier long identifier
   */
  GradoopId(Long identifier) {
    if (identifier == null) {
      throw new IllegalArgumentException("Identifier must not be null.");
    }
    this.identifier = identifier;
  }

  @Override
  public int hashCode() {
    return Objects.hash(identifier);
  }

  @Override
  public boolean equals(Object o) {
    boolean equals = false;

    if (this == o) {
      equals = true;
    } else if (o instanceof GradoopId) {
      equals = this.identifier.equals(((GradoopId) o).identifier);
    }

    return equals;
  }

  @Override
  public String toString() {
    return identifier.toString();
  }

  @Override
  public int compareTo(GradoopId otherId) {
    if (this == otherId) {
      return 0;
    }
    return this.identifier.compareTo(otherId.identifier);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(identifier);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.identifier = dataInput.readLong();
  }

  /**
   * factory method to create from a long value
   * @param id long
   * @return new id
   */
  public static GradoopId fromLong(Long id) {
    return new GradoopId(id);
  }

  /**
   * factory method to create from a string representing a long value
   * @param string long string
   * @return new id
   */
  public static GradoopId fromLongString(String string) {
    return GradoopId.fromLong(Long.valueOf(string));
  }

  /**
   * returns the smaller one of two given ids
   * @param firstId first id
   * @param secondId second id
   * @return smaller id
   */
  public static GradoopId min(GradoopId firstId, GradoopId secondId) {
    return firstId.compareTo(secondId) <= 0 ?
      firstId : secondId;
  }
}
