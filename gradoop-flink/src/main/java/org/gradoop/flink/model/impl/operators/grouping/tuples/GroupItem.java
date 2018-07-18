/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.grouping.tuples;

import org.gradoop.common.model.impl.properties.PropertyValueList;

/**
 * Interface for the vertex and the edge group item. Needed to use the same function to set the
 * standard values for these group items.
 */
public interface GroupItem {

  /**
   * Returns the label of the group of the group item.
   *
   * @return group label
   */
  String getGroupLabel();

  /**
   * Sets the label of the group of the group item.
   *
   * @param label new label
   */
  void setGroupLabel(String label);

  /**
   * Returns the label group of the group item.
   *
   * @return label group
   */
  LabelGroup getLabelGroup();

  /**
   * Sets the label group of the group item.
   *
   * @param labelGroup new label group
   */
  void setLabelGroup(LabelGroup labelGroup);

  /**
   * Returns the aggregate values of the group item.
   *
   * @return property value list of aggregates
   */
  PropertyValueList getAggregateValues();

  /**
   * Sets the aggregate values of the group item.
   *
   * @param aggregateValues property value list of aggregates
   */
  void setAggregateValues(PropertyValueList aggregateValues);

  /**
   * Returns the grouping values of the group item.
   *
   * @return property value list of grouping values
   */
  PropertyValueList getGroupingValues();

  /**
   * Sets the grouping values of the group item.
   *
   * @param groupPropertyValues property value list of grouping values
   */
  void setGroupingValues(PropertyValueList groupPropertyValues);


}
