
package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.gradoop.flink.model.impl.operators.grouping.tuples.EdgeGroupItem;

import java.io.IOException;

/**
 * Creates a single {@link EdgeGroupItem} from a set of group items.
 *
 * @see ReduceEdgeGroupItems
 * @see CombineEdgeGroupItems
 */
abstract class BuildSuperEdge extends BuildBase {

  /**
   * Creates group reducer / combiner
   *
   * @param useLabel    use edge label
   */
  public BuildSuperEdge(boolean useLabel) {
    super(useLabel);
  }

  /**
   * Iterators the given edge group items and build a group representative item.
   *
   * @param edgeGroupItems edge group items
   * @return group representative item
   */
  protected EdgeGroupItem reduceInternal(
    Iterable<EdgeGroupItem> edgeGroupItems) throws IOException {

    EdgeGroupItem edgeGroupItem = new EdgeGroupItem();
    boolean firstElement        = true;

    for (EdgeGroupItem edge : edgeGroupItems) {
      if (firstElement) {
        edgeGroupItem.setSourceId(edge.getSourceId());
        edgeGroupItem.setTargetId(edge.getTargetId());
        edgeGroupItem.setGroupLabel(edge.getGroupLabel());
        edgeGroupItem.setGroupingValues(edge.getGroupingValues());
        edgeGroupItem.setLabelGroup(edge.getLabelGroup());
        firstElement = false;
      }

      if (doAggregate(edgeGroupItem.getLabelGroup().getAggregators())) {
        aggregate(edge.getAggregateValues(), edgeGroupItem.getLabelGroup().getAggregators());
      } else {
        // no need to iterate further
        break;
      }
    }

    edgeGroupItem.setAggregateValues(
      getAggregateValues(edgeGroupItem.getLabelGroup().getAggregators()));
    return edgeGroupItem;
  }
}
