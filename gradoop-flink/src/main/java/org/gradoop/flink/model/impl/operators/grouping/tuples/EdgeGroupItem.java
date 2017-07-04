
package org.gradoop.flink.model.impl.operators.grouping.tuples;

import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValueList;

/**
 * Edge representation used for grouping edges to super edges.
 *
 * f0: source vertex id
 * f1: target vertex id
 * f2: edge group label
 * f3: edge group property values
 * f4: edge group aggregate values
 * f5: edge label group
 */
public class EdgeGroupItem
  extends Tuple6<GradoopId, GradoopId, String, PropertyValueList, PropertyValueList, LabelGroup>
  implements GroupItem {

  public GradoopId getSourceId() {
    return f0;
  }

  public void setSourceId(GradoopId sourceVertexId) {
    f0 = sourceVertexId;
  }

  public GradoopId getTargetId() {
    return f1;
  }

  public void setTargetId(GradoopId targetVertexId) {
    f1 = targetVertexId;
  }

  public String getGroupLabel() {
    return f2;
  }

  public void setGroupLabel(String groupLabel) {
    f2 = groupLabel;
  }

  public PropertyValueList getGroupingValues() {
    return f3;
  }

  public void setGroupingValues(PropertyValueList groupPropertyValues) {
    f3 = groupPropertyValues;
  }

  public PropertyValueList getAggregateValues() {
    return f4;
  }

  public void setAggregateValues(PropertyValueList value) {
    this.f4 = value;
  }

  public LabelGroup getLabelGroup() {
    return f5;
  }

  public void setLabelGroup(LabelGroup edgeLabelGroup) {
    f5 = edgeLabelGroup;
  }
}
