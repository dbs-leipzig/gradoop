package org.gradoop.flink.model.api.layouts;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.representation.transactional.GraphTransaction;

public interface GraphCollectionLayout extends Layout {

  /**
   * Returns the graph heads associated with the logical graphs in that
   * collection.
   *
   * @return graph heads
   */
  DataSet<GraphHead> getGraphHeads();

  /**
   * Returns the graph heads associated with the logical graphs in that
   * collection filtered by label.
   *
   * @param label graph head label
   * @return graph heads
   */
  DataSet<GraphHead> getGraphHeadsByLabel(String label);


  DataSet<GraphTransaction> getGraphTransactions();
}
