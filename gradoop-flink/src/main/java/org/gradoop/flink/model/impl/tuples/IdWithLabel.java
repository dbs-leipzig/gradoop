
package org.gradoop.flink.model.impl.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * (Id, Label)
 *
 * f0: id
 * f1: label
 */
public class IdWithLabel extends Tuple2<GradoopId, String> {

  /**
   * Constructor.
   *
   * @param id element id
   * @param label element label
   */
  public IdWithLabel(GradoopId id, String label) {
    super(id, label);
  }

  /**
   * Default Constructor
   */
  public IdWithLabel() {
  }

  public void setId(GradoopId id) {
    f0 = id;
  }

  public GradoopId getId() {
    return f0;
  }

  public void setLabel(String label) {
    f1 = label;
  }

  public String getLabel() {
    return f1;
  }
}
