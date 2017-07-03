
package org.gradoop.flink.io.impl.csv.tuples;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Tuple representing a vertex in a CSV file.
 */
public class CSVVertex extends Tuple3<String, String, String> {

  public String getId() {
    return f0;
  }

  public void setId(String id) {
    f0 = id;
  }

  public String getLabel() {
    return f1;
  }

  public void setLabel(String label) {
    f1 = label;
  }

  public String getProperties() {
    return f2;
  }

  public void setProperties(String properties) {
    f2 = properties;
  }
}
