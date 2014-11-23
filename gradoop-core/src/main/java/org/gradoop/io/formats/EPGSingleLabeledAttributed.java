package org.gradoop.io.formats;

import org.apache.hadoop.io.Writable;
import org.gradoop.model.SingleLabeled;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * Used to manage (de-)serialization of attributed entities that have one
 * label.
 */
public class EPGSingleLabeledAttributed extends EPGAttributedWritable
  implements SingleLabeled, Writable {

  private String label;

  public EPGSingleLabeledAttributed() {
  }

  public EPGSingleLabeledAttributed(final String label) {
    this(label, null);
  }

  public EPGSingleLabeledAttributed(final String label, final Map<String,
    Object> properties) {
    super(properties);
    this.label = label;
  }

  @Override
  public String getLabel() {
    return this.label;
  }

  @Override
  public void write(DataOutput dataOutput)
    throws IOException {
    super.write(dataOutput);
    dataOutput.writeUTF(label);
  }

  @Override
  public void readFields(DataInput dataInput)
    throws IOException {
    super.readFields(dataInput);
    label = dataInput.readUTF();
  }
}
