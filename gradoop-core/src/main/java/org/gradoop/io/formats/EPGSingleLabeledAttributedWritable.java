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
public class EPGSingleLabeledAttributedWritable extends EPGAttributedWritable
  implements SingleLabeled, Writable {

  private String label;

  public EPGSingleLabeledAttributedWritable() {
  }

  public EPGSingleLabeledAttributedWritable(final String label) {
    this(label, null);
  }

  public EPGSingleLabeledAttributedWritable(final String label,
                                            final Map<String,
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
    dataOutput.writeUTF(label);
    super.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput)
    throws IOException {
    label = dataInput.readUTF();
    super.readFields(dataInput);
  }
}
