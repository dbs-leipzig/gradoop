package org.gradoop.io.formats;

import org.apache.hadoop.io.Writable;
import org.gradoop.model.Labeled;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * Used to manage (de-)serialization of attributed entities that have one
 * label.
 */
public class EPGLabeledAttributedWritable extends
  EPGAttributedWritable implements Labeled, Writable {

  /**
   * Holds the label of that entity.
   */
  private String label;

  /**
   * Default constructor is necessary for object deserialization.
   */
  public EPGLabeledAttributedWritable() {
  }

  /**
   * Default constructor is necessary for object deserialization.
   *
   * @param label entity label
   */
  public EPGLabeledAttributedWritable(final String label) {
    this(label, null);
  }

  /**
   * Creates a labeled entity based on the given parameters.
   *
   * @param label      entity label (can be {@code null})
   * @param properties key-value-map (can be {@code null})
   */
  public EPGLabeledAttributedWritable(final String label,
    final Map<String, Object> properties) {
    super(properties);
    this.label = label;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getLabel() {
    return this.label;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setLabel(String label) {
    this.label = label;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(label);
    super.write(dataOutput);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void readFields(DataInput dataInput) throws IOException {
    label = dataInput.readUTF();
    super.readFields(dataInput);
  }
}
