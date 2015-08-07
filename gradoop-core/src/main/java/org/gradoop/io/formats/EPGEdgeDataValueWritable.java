///*
// * This file is part of Gradoop.
// *
// * Gradoop is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// *
// * Gradoop is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
// */
//
//package org.gradoop.io.formats;
//
//import org.gradoop.model.EdgeData;
//
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
//import java.util.Map;
//
///**
// * Used to manage (de-)serialization of edge values.
// */
//public class EPGEdgeDataValueWritable extends
//  EPGLabeledAttributedWritable implements EdgeData {
//
//  /**
//   * The vertex id of the "other" vertex the vertex storing that edge is
//   * connected to.
//   */
//  private Long otherID;
//
//  /**
//   * The vertex centric index of that edge, which is necessary to allow
// multiple
//   * edges between the same nodes with the same label.
//   */
//  private Long index;
//
//  /**
//   * Default constructor is necessary for object deserialization.
//   */
//  public EPGEdgeDataValueWritable() {
//  }
//
//  /**
//   * Copy constructor to create an Edge value from a given edge.
//   *
//   * @param edgeData edge to use as template for a new edge
//   */
//  public EPGEdgeDataValueWritable(final EdgeData edgeData) {
//    this(edgeData.getOtherID(), edgeData.getLabel(), edgeData.getIndex());
//    if (edgeData.getPropertyKeys() != null) {
//      for (String propertyKey : edgeData.getPropertyKeys()) {
//        this.setProperty(propertyKey, edgeData.getProperty(propertyKey));
//      }
//    }
//  }
//
//  /**
//   * Creates an edge value based on the given parameters.
//   *
//   * @param otherID the vertex this edge connects to
//   * @param label   edge label (must not be {@code null} or empty)
//   * @param index   internal index of that edge
//   */
//  public EPGEdgeDataValueWritable(final Long otherID, final String label,
//    final Long index) {
//    this(otherID, label, index, null);
//  }
//
//  /**
//   * Creates an edge value based on the given parameters.
//   *
//   * @param otherID    the vertex this edge connects to
//   * @param label      edge label (must not be {@code null} or empty)
//   * @param index      internal index of that edge
//   * @param properties key-value-map (can be {@code null})
//   */
//  public EPGEdgeDataValueWritable(final Long otherID, final String label,
//    final Long index, Map<String, Object> properties) {
//    super(label, properties);
//    this.otherID = otherID;
//    this.index = index;
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public Long getOtherID() {
//    return this.otherID;
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public Long getIndex() {
//    return this.index;
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public void write(DataOutput dataOutput) throws IOException {
//    dataOutput.writeLong(this.otherID);
//    dataOutput.writeLong(this.index);
//    super.write(dataOutput);
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public void readFields(DataInput dataInput) throws IOException {
//    this.otherID = dataInput.readLong();
//    this.index = dataInput.readLong();
//    super.readFields(dataInput);
//  }
//}
