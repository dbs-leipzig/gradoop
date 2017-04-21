package org.gradoop.benchmark.nesting.serializers.data;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.BinaryInputFormat;
import org.apache.flink.api.common.io.BinaryOutputFormat;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;

import java.io.IOException;
import java.util.Iterator;

/**
 * Serializing a vertex in a proper way
 */
public class DeserializeVertexInformation<VH extends EPGMElement> implements MapFunction<String, VH> {

  /**
   * Reusable element
   */
  private final VH v;

  /**
   * Default constructor
   */
  public DeserializeVertexInformation(VH emptyElement) {
    v = emptyElement;
    v.setProperties(new Properties());
  }

  @Override
  public VH map(String value) throws Exception {
    String[] array = value.split(",");
    v.setId(GradoopId.fromString(array[0]));
    if (array[1].startsWith("\"") && array[1].endsWith("\"")) {
      v.setLabel(array[1].substring(1, array[1].length()-1));
    } else {
      v.setLabel(null);
    }
    v.getProperties().clear();
    if (array.length > 2) {
      for (int i=2; i<array.length; i += 2) {
        v.setProperty(array[i], array[i+1]);
      }
    }
    return v;
  }
}
