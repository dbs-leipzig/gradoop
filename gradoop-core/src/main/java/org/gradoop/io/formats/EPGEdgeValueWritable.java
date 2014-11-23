package org.gradoop.io.formats;

import java.util.Map;

/**
 * Used to manage (de-)serialization of edge values.
 */
public class EPGEdgeValueWritable extends EPGSingleLabeledAttributed {

  public EPGEdgeValueWritable() {
  }

  public EPGEdgeValueWritable(String label) {
    super(label);
  }

  public EPGEdgeValueWritable(String label,
                              Map<String, Object> properties) {
    super(label, properties);
  }
}
