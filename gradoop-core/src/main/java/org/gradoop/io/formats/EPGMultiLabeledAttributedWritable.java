package org.gradoop.io.formats;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.Writable;
import org.gradoop.model.MultiLabeled;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Used to manage (de-)serialization of attributed entities that can have
 * multiple labels.
 */
public class EPGMultiLabeledAttributedWritable extends EPGAttributedWritable
  implements MultiLabeled, Writable {

  /**
   * Holds all labels of that entity.
   */
  private List<String> labels;

  /**
   * Default constructor is necessary for object deserialization.
   */
  public EPGMultiLabeledAttributedWritable() {
    labels = Lists.newArrayList();
  }

  /**
   * Creates a multi labeled entity based on the given values.
   *
   * @param labels     initial list of labels (can be {@null})
   * @param properties key-value-map (can be {@null})
   */
  public EPGMultiLabeledAttributedWritable(Iterable<String> labels, Map<String,
    Object> properties) {
    super(properties);
    this.labels = (labels != null) ? Lists.newArrayList(labels) : null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterable<String> getLabels() {
    return labels;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addLabel(String label) {
    if (label == null || "".equals(label)) {
      throw new IllegalArgumentException("label must not be null or empty");
    }
    if (this.labels == null) {
      this.labels = Lists.newArrayList();
    }
    this.labels.add(label);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(DataOutput dataOutput)
    throws IOException {
    dataOutput.writeInt(labels.size());
    for (String label : labels) {
      dataOutput.writeUTF(label);
    }
    super.write(dataOutput);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void readFields(DataInput dataInput)
    throws IOException {
    // labels
    final int labelCount = dataInput.readInt();
    for (int i = 0; i < labelCount; i++) {
      labels.add(dataInput.readUTF());
    }
    super.readFields(dataInput);
  }
}
