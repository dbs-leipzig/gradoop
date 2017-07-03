
package org.gradoop.flink.model.impl.operators.selection;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.impl.GraphCollection;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Filter logical graphs from a graph collection based on their associated graph
 * head.
 */
public class Selection extends SelectionBase {

  /**
   * User-defined predicate function
   */
  private final FilterFunction<GraphHead> predicate;

  /**
   * Creates a new Selection operator.
   *
   * @param predicate user-defined predicate function
   */
  public Selection(FilterFunction<GraphHead> predicate) {
    this.predicate = checkNotNull(predicate, "Predicate function was null");
  }

  @Override
  public GraphCollection execute(GraphCollection collection) {

    // find graph heads matching the predicate
    DataSet<GraphHead> graphHeads = collection
      .getGraphHeads()
      .filter(predicate);

    return selectVerticesAndEdges(collection, graphHeads);
  }

  @Override
  public String getName() {
    return Selection.class.getName();
  }
}
