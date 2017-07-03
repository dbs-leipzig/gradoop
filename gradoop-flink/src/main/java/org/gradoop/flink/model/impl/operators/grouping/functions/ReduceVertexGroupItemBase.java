
package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexGroupItem;
import org.gradoop.common.model.impl.properties.PropertyValueList;

import java.io.IOException;
import java.util.List;

/**
 * Base class for reducer/combiner implementations on vertices.
 */
abstract class ReduceVertexGroupItemBase extends BuildBase {
  /**
   * Reduce instantiations
   */
  private final VertexGroupItem reuseVertexGroupItem;

  /**
   * Creates build base.
   *
   * @param useLabel true, if element label shall be used for grouping
   */
  protected ReduceVertexGroupItemBase(boolean useLabel) {
    super(useLabel);
    this.reuseVertexGroupItem = new VertexGroupItem();
  }

  protected VertexGroupItem getReuseVertexGroupItem() {
    return this.reuseVertexGroupItem;
  }

  /**
   * Creates one super vertex tuple representing the whole group. This tuple is
   * later used to create a super vertex for each group.
   *
   * @param superVertexId       super vertex id
   * @param groupLabel          group label
   * @param groupPropertyValues group property values
   * @param valueAggregators    vertex aggregators
   * @return vertex group item representing the super vertex
   */
  protected VertexGroupItem createSuperVertexTuple(
    GradoopId superVertexId, String groupLabel,
    PropertyValueList groupPropertyValues,
    List<PropertyValueAggregator> valueAggregators) throws IOException {
    reuseVertexGroupItem.setVertexId(superVertexId);
    reuseVertexGroupItem.setGroupLabel(groupLabel);
    reuseVertexGroupItem.setGroupingValues(groupPropertyValues);
    reuseVertexGroupItem.setAggregateValues(getAggregateValues(valueAggregators));
    reuseVertexGroupItem.setSuperVertex(true);
    return reuseVertexGroupItem;
  }
}
