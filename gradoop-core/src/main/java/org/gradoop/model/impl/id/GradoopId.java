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

import com.sun.istack.NotNull;
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
    if (this == o) {
      return true;
    }
    if (!(o instanceof GradoopId)) {
      return false;
    }
    GradoopId gradoopId = (GradoopId) o;
    return Objects.equals(identifier, gradoopId.identifier);
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
}
