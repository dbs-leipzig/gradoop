
package org.gradoop.flink.io.impl.csv.tuples;

import org.apache.flink.api.java.tuple.Tuple5;

/**
 * Tuple representing an edge in a CSV file.
 */
public class CSVEdge extends Tuple5<String, String, String, String, String> {

  public String getId() {
    return f0;
  }

  public void setId(String id) {
    f0 = id;
  }

  public String getSourceId() {
    return f1;
  }

  public void setSourceId(String sourceId) {
    f1 = sourceId;
  }

  public String getTargetId() {
    return f2;
  }

  public void setTargetId(String targetId) {
    f2 = targetId;
  }

  public String getLabel() {
    return f3;
  }

  public void setLabel(String label) {
    f3 = label;
  }

  public String getProperties() {
    return f4;
  }

  public void setProperties(String properties) {
    f4 = properties;
  }
}
