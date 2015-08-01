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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.io.formats;

import org.apache.hadoop.io.WritableComparable;
import org.gradoop.model.Identifiable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Stores the globally unique vertex identifier.
 */
public class EPGVertexIdentifierWritable implements Identifiable,
  WritableComparable<EPGVertexIdentifierWritable> {

  /**
   * A globally unique identifier for a vertex.
   */
  private Long id;

  /**
   * Default constructor is necessary for object deserialization.
   */
  public EPGVertexIdentifierWritable() {
  }

  /**
   * Creates a vertex identifier based on the given parameter.
   *
   * @param id globally unique long value
   */
  public EPGVertexIdentifierWritable(Long id) {
    this.id = id;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Long getId() {
    return id;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setId(Long id) {
    this.id = id;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(id);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.id = dataInput.readLong();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int compareTo(EPGVertexIdentifierWritable o) {
    if (this == o) {
      return 0;
    }
    return Long.compare(this.getId(), o.getId());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    EPGVertexIdentifierWritable that = (EPGVertexIdentifierWritable) o;

    if (id != null ? !id.equals(that.id) : that.id != null) {
      return false;
    }
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    return id != null ? id.hashCode() : 0;
  }
}
