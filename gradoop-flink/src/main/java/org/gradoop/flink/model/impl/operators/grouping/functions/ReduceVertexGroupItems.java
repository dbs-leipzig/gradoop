
package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexGroupItem;
import org.gradoop.common.model.impl.properties.PropertyValueList;

/**
 * Reduces a group of {@link VertexGroupItem} instances.
 */
@FunctionAnnotation.ForwardedFields(
    "f0;" + // vertex id
    "f2;" + // label
    "f3;" + // properties
    "f4;" + // aggregates
    "f6"    // label group
)
public class ReduceVertexGroupItems
  extends ReduceVertexGroupItemBase
  implements GroupReduceFunction<VertexGroupItem, VertexGroupItem> {

  /**
   * Creates group reduce function.
   *
   * @param useLabel true, iff labels are used for grouping
   */
  public ReduceVertexGroupItems(boolean useLabel) {
    super(useLabel);
  }

  @Override
  public void reduce(Iterable<VertexGroupItem> vertexGroupItems,
    Collector<VertexGroupItem> collector) throws Exception {

    GradoopId superVertexId                         = null;
    String groupLabel                               = null;
    PropertyValueList groupPropertyValues           = null;

    VertexGroupItem reuseTuple = getReuseVertexGroupItem();

    boolean isFirst = true;

    for (VertexGroupItem groupItem : vertexGroupItems) {
      if (isFirst) {
        superVertexId       = GradoopId.get();
        groupLabel          = groupItem.getGroupLabel();
        groupPropertyValues = groupItem.getGroupingValues();

        reuseTuple.setGroupLabel(groupLabel);
        reuseTuple.setGroupingValues(groupPropertyValues);
        reuseTuple.setSuperVertexId(superVertexId);
        reuseTuple.setAggregateValues(groupItem.getAggregateValues());
        reuseTuple.setSuperVertex(groupItem.isSuperVertex());
        reuseTuple.setLabelGroup(groupItem.getLabelGroup());

        isFirst = false;
      }
      reuseTuple.setVertexId(groupItem.getVertexId());

      // collect updated vertex item
      collector.collect(reuseTuple);

      if (doAggregate(groupItem.getLabelGroup().getAggregators())) {
        aggregate(groupItem.getAggregateValues(), reuseTuple.getLabelGroup().getAggregators());
      }
    }

    VertexGroupItem superVertex = createSuperVertexTuple(
      superVertexId,
      groupLabel,
      groupPropertyValues,
      reuseTuple.getLabelGroup().getAggregators());
    resetAggregators(superVertex.getLabelGroup().getAggregators());
    collector.collect(superVertex);
  }
}
