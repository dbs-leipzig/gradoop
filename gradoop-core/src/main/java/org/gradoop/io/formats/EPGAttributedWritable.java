package org.gradoop.io.formats;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.gradoop.model.Attributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * Used to manage (de-)serialization of attributed entities.
 */
public class EPGAttributedWritable implements Attributed, Writable {

  private Map<String, Object> properties;

  public EPGAttributedWritable() {
    this(null);
  }

  public EPGAttributedWritable(Map<String, Object> properties) {
    this.properties = properties;
  }

  @Override
  public Iterable<String> getPropertyKeys() {
    return (properties != null) ? properties.keySet() : null;
  }

  @Override
  public Object getProperty(String key) {
    return (properties != null) ? properties.get(key) : null;
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
      initProperties();
    }
    this.properties.put(key, value);
  }

  private void initProperties() {
    initProperties(-1);
  }

  private void initProperties(int expectedSize) {
    if (expectedSize >= 0) {
      this.properties = Maps.newHashMapWithExpectedSize(expectedSize);
    } else {
      this.properties = Maps.newHashMap();
    }
  }

  @Override
  public void write(DataOutput dataOutput)
    throws IOException {
    if (properties != null) {
      dataOutput.writeInt(properties.size());
      ObjectWritable ow = new ObjectWritable();
      for (Map.Entry<String, Object> property : properties.entrySet()) {
        dataOutput.writeUTF(property.getKey());
        ow.set(property.getValue());
        ow.write(dataOutput);
      }
    } else {
      dataOutput.writeInt(0);
    }
  }

  @Override
  public void readFields(DataInput dataInput)
    throws IOException {
    ObjectWritable ow = new ObjectWritable();
    Configuration conf = new Configuration();
    ow.setConf(conf);

    final int propertyCount = dataInput.readInt();
    if (propertyCount > 0) {
      initProperties(propertyCount);

      for (int i = 0; i < propertyCount; i++) {
        String key = dataInput.readUTF();
        ow.readFields(dataInput);
        Object value = ow.get();
        properties.put(key, value);
      }
    } else {
      /*
      The properties map has to be initialized even if there are no
      properties at the element.
      Not initializing the properties leads to wrong behaviour in giraph
      where edges with no properties (null) have properties from other edges.

      This is of course a huge memory overhead as there are n + m (possibly
      empty) HashMaps in a graph with n vertices and m edges.
       */
      initProperties();
    }
  }
}