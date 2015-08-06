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
//import org.apache.hadoop.io.Writable;
//import org.gradoop.model.Labeled;
//
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
//import java.util.Map;
//
///**
// * Used to manage (de-)serialization of attributed entities that have one
// * label.
// */
//public class EPGLabeledAttributedWritable extends
//  EPGAttributedWritable implements Labeled, Writable {
//
//  /**
//   * Holds the label of that entity.
//   */
//  private String label;
//
//  /**
//   * Default constructor is necessary for object deserialization.
//   */
//  public EPGLabeledAttributedWritable() {
//  }
//
//  /**
//   * Default constructor is necessary for object deserialization.
//   *
//   * @param label entity label
//   */
//  public EPGLabeledAttributedWritable(final String label) {
//    this(label, null);
//  }
//
//  /**
//   * Creates a labeled entity based on the given parameters.
//   *
//   * @param label      entity label (can be {@code null})
//   * @param properties key-value-map (can be {@code null})
//   */
//  public EPGLabeledAttributedWritable(final String label,
//    final Map<String, Object> properties) {
//    super(properties);
//    this.label = label;
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public String getLabel() {
//    return this.label;
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public void setLabel(String label) {
//    this.label = label;
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public void write(DataOutput dataOutput) throws IOException {
//    dataOutput.writeUTF(label);
//    super.write(dataOutput);
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public void readFields(DataInput dataInput) throws IOException {
//    label = dataInput.readUTF();
//    super.readFields(dataInput);
//  }
//}
