
package org.gradoop.flink.io.impl.csv.metadata;

import java.util.List;
import java.util.Map;

/**
 * Describes the data stored in the vertex and edge CSV files.
 */
public class MetaData {
  /**
   * Mapping between an element label and its associated property meta data.
   */
  private Map<String, List<PropertyMetaData>> metaData;

  /**
   * Constructor
   *
   * @param metaData meta data
   */
  MetaData(Map<String, List<PropertyMetaData>> metaData) {
    this.metaData = metaData;
  }

  /**
   * Returns the property meta data associated with the specified label.
   *
   * @param label element label
   * @return property meta data for the element
   */
  public List<PropertyMetaData> getPropertyMetaData(String label) {
    return metaData.get(label);
  }
}
