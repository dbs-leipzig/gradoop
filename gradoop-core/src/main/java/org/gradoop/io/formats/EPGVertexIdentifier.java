package org.gradoop.io.formats;

import org.apache.hadoop.io.WritableComparable;
import org.gradoop.model.Identifiable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by martin on 20.11.14.
 */
public class EPGVertexIdentifier implements Identifiable,
  WritableComparable<EPGVertexIdentifier> {

  private Long id;

  public EPGVertexIdentifier() {
  }

  public EPGVertexIdentifier(Long id) {
    this.id = id;
  }

  @Override
  public Long getID() {
    return id;
  }

  @Override
  public void write(DataOutput dataOutput)
    throws IOException {
    dataOutput.writeLong(id);
  }

  @Override
  public void readFields(DataInput dataInput)
    throws IOException {
    this.id = dataInput.readLong();
  }

  @Override
  public int compareTo(EPGVertexIdentifier o) {
    if (this == o) {
      return 0;
    }
    return Long.compare(this.getID(), o.getID());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    EPGVertexIdentifier that = (EPGVertexIdentifier) o;

    if (id != null ? !id.equals(that.id) : that.id != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return id != null ? id.hashCode() : 0;
  }
}
