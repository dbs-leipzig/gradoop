package org.gradoop.io.formats;

import org.gradoop.model.Edge;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * Used to manage (de-)serialization of edge values.
 */
public class EPGEdgeValueWritable extends EPGSingleLabeledAttributedWritable
  implements Edge {

  private Long otherID;

  private Long index;

  public EPGEdgeValueWritable() {
  }

  /**
   * Copy constructor to clone an edge.
   *
   * @param edge edge to use as template for a new edge
   */
  public EPGEdgeValueWritable(final Edge edge) {
    this(edge.getOtherID(), edge.getLabel(), edge.getIndex());
    if (edge.getPropertyKeys() != null) {
      for (String propertyKey : edge.getPropertyKeys()) {
        this.addProperty(propertyKey, edge.getProperty(propertyKey));
      }
    }
  }

  public EPGEdgeValueWritable(final Long otherID, final String label,
                              final Long index) {
    this(otherID, label, index, null);
  }

  public EPGEdgeValueWritable(final Long otherID, final String label,
                              final Long index, Map<String,
    Object> properties) {
    super(label, properties);
    this.otherID = otherID;
    this.index = index;
  }

  @Override
  public Long getOtherID() {
    return this.otherID;
  }

  @Override
  public Long getIndex() {
    return this.index;
  }

  @Override
  public void write(DataOutput dataOutput)
    throws IOException {
    dataOutput.writeLong(this.otherID);
    dataOutput.writeLong(this.index);
    super.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput)
    throws IOException {
    this.otherID = dataInput.readLong();
    this.index = dataInput.readLong();
    super.readFields(dataInput);
  }
}
