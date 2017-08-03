package org.gradoop.flink.model.api.layouts;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.GraphHead;

/**
 * A logical graph layout defines the Flink internal (DataSet) representation of a
 * {@link org.gradoop.flink.model.api.epgm.LogicalGraph}.
 */
public interface LogicalGraphLayout extends Layout {

  /**
   * Returns a dataset containing a single graph head associated with that
   * logical graph.
   *
   * @return 1-element dataset
   */
  DataSet<GraphHead> getGraphHead();
}
