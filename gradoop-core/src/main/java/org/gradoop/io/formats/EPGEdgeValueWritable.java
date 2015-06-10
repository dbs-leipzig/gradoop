package org.gradoop.io.formats;

import org.gradoop.model.Edge;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * Used to manage (de-)serialization of edge values.
 */
public class EPGEdgeValueWritable extends
  EPGLabeledAttributedWritable implements Edge {

  /**
   * The vertex id of the "other" vertex the vertex storing that edge is
   * connected to.
   */
  private Long otherID;

  /**
   * The vertex centric index of that edge, which is necessary to allow multiple
   * edges between the same nodes with the same label.
   */
  private Long index;

  /**
   * Default constructor is necessary for object deserialization.
   */
  public EPGEdgeValueWritable() {
  }

  /**
   * Copy constructor to create an Edge value from a given edge.
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

  /**
   * Creates an edge value based on the given parameters.
   *
   * @param otherID the vertex this edge connects to
   * @param label   edge label (must not be {@code null} or empty)
   * @param index   internal index of that edge
   */
  public EPGEdgeValueWritable(final Long otherID, final String label,
    final Long index) {
    this(otherID, label, index, null);
  }

  /**
   * Creates an edge value based on the given parameters.
   *
   * @param otherID    the vertex this edge connects to
   * @param label      edge label (must not be {@code null} or empty)
   * @param index      internal index of that edge
   * @param properties key-value-map (can be {@code null})
   */
  public EPGEdgeValueWritable(final Long otherID, final String label,
    final Long index, Map<String, Object> properties) {
    super(label, properties);
    this.otherID = otherID;
    this.index = index;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Long getOtherID() {
    return this.otherID;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Long getIndex() {
    return this.index;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(this.otherID);
    dataOutput.writeLong(this.index);
    super.write(dataOutput);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.otherID = dataInput.readLong();
    this.index = dataInput.readLong();
    super.readFields(dataInput);
  }
}
