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

import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation
  .PropertyValueAggregator;

import java.util.List;

/**
 * Stores grouping keys for a specific label.
 */
public class LabelGroup
  extends Tuple4<String, String, List<String>, List<PropertyValueAggregator>> {

  /**
   * Default constructor.
   */
  public LabelGroup() {
    this(null, null);
  }

  /**
   * Constructor to only define the label.
   *
   * @param groupLabel    label used after grouping
   * @param groupingLabel label used for grouping
   */
  public LabelGroup(String groupLabel, String groupingLabel) {
    this(groupLabel, groupingLabel, Lists.newArrayList(), Lists.newArrayList());
  }

  /**
   * Constructor with varargs.
   *
   * @param groupLabel    label used after grouping
   * @param groupingLabel label used for grouping
   * @param propertyKeys variable amount of grouping keys for the label
   * @param aggregators  aggregate functions
   */
  public LabelGroup(
    String groupLabel, String groupingLabel,
    List<String> propertyKeys,
    List<PropertyValueAggregator> aggregators) {
    super(groupLabel, groupingLabel, propertyKeys, aggregators);
  }

  public String getGroupLabel() {
    return f0;
  }

  public void setGroupLabel(String label) {
    f0 = label;
  }

  public String getGroupingLabel() {
    return f1;
  }

  public void setGroupingLabel(String label) {
    f1 = label;
  }

  public List<String> getPropertyKeys() {
    return f2;
  }

  public void setPropertyKeys(List<String> propertyKeys) {
    f2 = propertyKeys;
  }

  /**
   * Adds a property key to the current list of keys.
   *
   * @param propertyKey property key as string
   */
  public void addPropertyKey(String propertyKey) {
    f2.add(propertyKey);
  }

  public List<PropertyValueAggregator> getAggregators() {
    return f3;
  }

  public void setAggregators(List<PropertyValueAggregator> aggregators) {
    f3 = aggregators;
  }

  /**
   * Adds an aggregator to the current list of aggregators.
   *
   * @param aggregator property value aggregator
   */
  public void addAggregator(PropertyValueAggregator aggregator) {
    f3.add(aggregator);
  }
}
