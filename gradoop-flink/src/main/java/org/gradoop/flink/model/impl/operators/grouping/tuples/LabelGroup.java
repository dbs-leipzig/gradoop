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
   * @param groupingLabel label used for grouping
   * @param groupLabel    label used after grouping
   */
  public LabelGroup(String groupingLabel, String groupLabel) {
    this(groupingLabel, groupLabel, Lists.newArrayList(), Lists.newArrayList());
  }

  /**
   * Constructor with varargs.
   *
   * @param groupingLabel label used for grouping
   * @param groupLabel    label used after grouping
   * @param propertyKeys variable amount of grouping keys for the label
   * @param aggregators  aggregate functions
   */
  public LabelGroup(
    String groupingLabel, String groupLabel,
    List<String> propertyKeys,
    List<PropertyValueAggregator> aggregators) {
    super(groupingLabel, groupLabel, propertyKeys, aggregators);
  }

  public String getGroupingLabel() {
    return f0;
  }

  public void setGroupingLabel(String label) {
    f0 = label;
  }

  public String getGroupLabel() {
    return f1;
  }

  public void setGroupLabel(String label) {
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
