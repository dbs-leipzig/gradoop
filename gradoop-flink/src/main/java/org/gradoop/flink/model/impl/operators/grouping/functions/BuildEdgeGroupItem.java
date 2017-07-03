
package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.grouping.tuples.EdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;

import java.util.List;

/**
 * Takes an EPGM edge as input and creates a {@link EdgeGroupItem} which
 * contains only necessary information for further processing.
 *
 */
@FunctionAnnotation.ForwardedFields("sourceId->f0;targetId->f1;")
@FunctionAnnotation.ReadFields("label;properties")
public class BuildEdgeGroupItem
  extends BuildGroupItemBase
  implements FlatMapFunction<Edge, EdgeGroupItem> {

  /**
   * Avoid object initialization in each call.
   */
  private final EdgeGroupItem reuseEdgeGroupItem;

  /**
   * Creates map function.
   *
   * @param useLabel                        true, if vertex label shall be used
   * @param edgeLabelGroups                 stores grouping properties for edge labels
   */
  public BuildEdgeGroupItem(boolean useLabel, List<LabelGroup> edgeLabelGroups) {
    super(useLabel, edgeLabelGroups);
    this.reuseEdgeGroupItem = new EdgeGroupItem();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flatMap(Edge edge, Collector<EdgeGroupItem> collector) throws Exception {
    boolean usedEdgeLabelGroup = false;
    reuseEdgeGroupItem.setSourceId(edge.getSourceId());
    reuseEdgeGroupItem.setTargetId(edge.getTargetId());

    // check if edge shall be grouped by a special set of keys
    for (LabelGroup edgeLabelGroup : getLabelGroups()) {
      if (edgeLabelGroup.getGroupingLabel().equals(edge.getLabel())) {
        usedEdgeLabelGroup = true;
        setGroupItem(reuseEdgeGroupItem, edge, edgeLabelGroup);
        collector.collect(reuseEdgeGroupItem);
      }
    }
    // standard grouping case
    if (!usedEdgeLabelGroup) {
      setGroupItem(reuseEdgeGroupItem, edge, getDefaultLabelGroup());
      collector.collect(reuseEdgeGroupItem);
    }
  }
}
