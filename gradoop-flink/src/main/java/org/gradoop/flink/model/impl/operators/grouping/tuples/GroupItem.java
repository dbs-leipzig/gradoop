/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
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
