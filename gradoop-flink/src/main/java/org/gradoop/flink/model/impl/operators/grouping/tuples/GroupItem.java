package org.gradoop.flink.model.impl.operators.grouping.tuples;

import org.gradoop.common.model.impl.properties.PropertyValueList;

/**
 * Created by stephan on 17.05.17.
 */
public interface GroupItem {

  String getGroupLabel();

  void setGroupLabel(String label);

  LabelGroup getLabelGroup();

  void setLabelGroup(LabelGroup labelGroup);

  PropertyValueList getAggregateValues();

  void setAggregateValues(PropertyValueList aggregateValues);

  void setGroupingValues(PropertyValueList groupPropertyValues);

  PropertyValueList getGroupingValues();

}
