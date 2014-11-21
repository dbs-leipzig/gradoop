package org.gradoop.io.formats;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.gradoop.model.Attributed;
import org.gradoop.model.MultiLabeled;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by martin on 20.11.14.
 */
public class EPGMultiLabeledAttributedWritable implements MultiLabeled,
  Attributed, Writable {

  private List<String> labels;

  private Map<String, Object> properties;

  public EPGMultiLabeledAttributedWritable() {
    labels = Lists.newArrayList();
    properties = Maps.newHashMap();
  }

  public EPGMultiLabeledAttributedWritable(Iterable<String> labels, Map<String,
    Object> properties) {
    this.labels = (labels != null) ? Lists.newArrayList(labels) : null;
    this.properties = properties;
  }

  @Override
  public Iterable<String> getPropertyKeys() {
    return properties.keySet();
  }

  @Override
  public Object getProperty(String key) {
    return properties.get(key);
  }

  @Override
  public void addProperty(String key, Object value) {
    if (key == null || "".equals(key)) {
      throw new IllegalArgumentException("key must not be null or empty");
    }
    if (value == null) {
      throw new IllegalArgumentException("value must not be null");
    }
    if (this.properties == null) {
      this.properties = Maps.newHashMap();
    }
    this.properties.put(key, value);
  }

  @Override
  public Iterable<String> getLabels() {
    return labels;
  }

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

  @Override
  public void write(DataOutput dataOutput)
    throws IOException {
    // labels
    dataOutput.writeInt(labels.size());
    for (String label : labels) {
      dataOutput.writeUTF(label);
    }
    // properties
    dataOutput.writeInt(properties.size());
    ObjectWritable ow = new ObjectWritable();
    for (Map.Entry<String, Object> property : properties.entrySet()) {
      dataOutput.writeUTF(property.getKey());
      ow.set(property.getValue());
      ow.write(dataOutput);
    }
  }

  @Override
  public void readFields(DataInput dataInput)
    throws IOException {
    // labels
    final int labelCount = dataInput.readInt();
    for (int i = 0; i < labelCount; i++) {
      labels.add(dataInput.readUTF());
    }

    // properties
    ObjectWritable ow = new ObjectWritable();
    Configuration conf = new Configuration();
    ow.setConf(conf);

    final int propertyCount = dataInput.readInt();
    for (int i = 0; i < propertyCount; i++) {
      String key = dataInput.readUTF();
      ow.readFields(dataInput);
      Object value = ow.get();
      properties.put(key, value);
    }
  }
}
