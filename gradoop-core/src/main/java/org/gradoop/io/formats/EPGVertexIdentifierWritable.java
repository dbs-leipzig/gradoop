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
  public Long getID() {
    return id;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(DataOutput dataOutput)
    throws IOException {
    dataOutput.writeLong(id);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void readFields(DataInput dataInput)
    throws IOException {
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
    return Long.compare(this.getID(), o.getID());
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
