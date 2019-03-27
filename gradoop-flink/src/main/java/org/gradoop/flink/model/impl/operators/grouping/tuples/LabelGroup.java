/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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

import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueList;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.impl.operators.aggregation.functions.AggregateUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Collectors;

/**
 * Stores grouping keys and aggregates for a specific label.
 *
 * <pre>
 * f0: grouping label
 * f1: group label
 * f2: property keys
 * f3: aggregate functions
 * f4: aggregate values
 * </pre>
 */
public class LabelGroup
  extends Tuple5<String, String, List<String>, List<AggregateFunction>, List<PropertyValue>> {

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
   * @param groupLabel label used after grouping
   */
  public LabelGroup(String groupingLabel, String groupLabel) {
    this(groupingLabel, groupLabel, new ArrayList<>(), new ArrayList<>());
  }

  /**
   * Constructor with varargs.
   *
   * @param groupingLabel label used for grouping
   * @param groupLabel label used after grouping
   * @param propertyKeys variable amount of grouping keys for the label
   * @param aggregators aggregate functions
   */
  public LabelGroup(
    String groupingLabel, String groupLabel,
    List<String> propertyKeys,
    List<AggregateFunction> aggregators) {
    super(groupingLabel, groupLabel, propertyKeys, aggregators, new ArrayList<>());
  }

  /**
   * Returns the grouping label
   *
   * @return grouping label
   */
  public String getGroupingLabel() {
    return f0;
  }

  /**
   * Sets the grouping label
   *
   * @param label grouping label
   */
  public void setGroupingLabel(String label) {
    f0 = label;
  }

  /**
   * Returns the group label
   *
   * @return group label
   */
  public String getGroupLabel() {
    return f1;
  }

  /**
   * Sets the group label
   *
   * @param label group label
   */
  public void setGroupLabel(String label) {
    f1 = label;
  }

  /**
   * Returns the property keys
   *
   * @return list of property keys
   */
  public List<String> getPropertyKeys() {
    return f2;
  }

  /**
   * Sets the property keys
   *
   * @param propertyKeys list of property keys
   */
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

  /**
   * Returns the aggregate functions as list
   *
   * @return list of aggregate functions
   */
  public List<AggregateFunction> getAggregateFunctions() {
    return f3;
  }

  /**
   * Sets the aggregate functions
   *
   * @param aggregateFunctions aggregate functions
   */
  public void setAggregateFunctions(List<AggregateFunction> aggregateFunctions) {
    f3 = aggregateFunctions;
  }

  /**
   * Adds an aggregate function to the current list of aggregators.
   *
   * @param aggregateFunction property value aggregate function
   */
  public void addAggregateFunction(AggregateFunction aggregateFunction) {
    f3.add(aggregateFunction);
  }

  /**
   * Returns the aggregate values as list
   *
   * @return aggregate values
   */
  public List<PropertyValue> getAggregateValues() {
    if (f4.size() < f3.size()) {
      return f3.stream().map(AggregateUtil::getDefaultAggregate).collect(Collectors.toList());
    }
    return f4;
  }

  /**
   * Sets the aggregate values
   *
   * @param aggregateValues list of aggregate values
   */
  public void setAggregateValues(List<PropertyValue> aggregateValues) {
    f4 = aggregateValues;
  }

  /**
   * Sets the aggregate values
   *
   * @param aggregateValues aggregate value list
   */
  public void setAggregateValues(PropertyValueList aggregateValues) {
    List<PropertyValue> aggregate = new ArrayList<>();
    aggregateValues.iterator().forEachRemaining(aggregate::add);
    setAggregateValues(aggregate);
  }

  /**
   * Resets the current aggregate values
   */
  public void resetAggregateValues() {
    f4.clear();
  }

  /**
   * Returns the aggregate values as property value list
   *
   * @return aggregate values
   * @throws IOException on failure
   */
  public PropertyValueList getAggregateValueList() throws IOException {
    return PropertyValueList.fromPropertyValues(getAggregateValues());
  }

  /**
   * Aggregates the aggregate values with {@code values} using the aggregate functions
   *
   * @param values values to aggregate with
   */
  public void aggregate(PropertyValueList values) {
    if (f4.isEmpty()) {
      setAggregateValues(values);
      return;
    }

    Iterator<PropertyValue> valueIt = values.iterator();
    ListIterator<PropertyValue> aggregateIt = f4.listIterator();
    PropertyValue value;
    PropertyValue aggregate;
    for (AggregateFunction valueAggregator : getAggregateFunctions()) {
      value = valueIt.next();
      if (!PropertyValue.NULL_VALUE.equals(value)) {
        aggregate = aggregateIt.next();
        if (!PropertyValue.NULL_VALUE.equals(aggregate)) {
          aggregateIt.set(valueAggregator.aggregate(aggregate, value));
        } else {
          aggregateIt.set(value);
        }
      }
    }
  }

  /**
   * Returns the property values of the given element which are used for
   * aggregation. If the EPGM element does not have a property, it uses the
   * default value or {@code PropertyValue.NULL_VALUE} instead.
   *
   * @param element attributed EPGM element
   * @return property values for aggregation
   */
  public PropertyValueList getIncrementValues(EPGMElement element) throws IOException {
    if (f3.isEmpty()) {
      return PropertyValueList.createEmptyList();
    }
    List<PropertyValue> propertyValues = getAggregateFunctions().stream()
      .map(f -> getIncrement(f, element))
      .collect(Collectors.toList());
    return PropertyValueList.fromPropertyValues(propertyValues);
  }

  /**
   * Returns the increment value for an aggregate function and an element.
   *
   * @param aggregateFunction aggregate function to create the increment
   * @param element element used to create the increment
   * @return increment value
   */
  private static PropertyValue getIncrement(AggregateFunction aggregateFunction,
                                            EPGMElement element) {
    PropertyValue increment = null;
    if ((element instanceof EPGMVertex && aggregateFunction.isVertexAggregation()) ||
      (element instanceof EPGMEdge && aggregateFunction.isEdgeAggregation())) {
      increment = aggregateFunction.getIncrement((Element) element);
    }
    return increment == null ? AggregateUtil.getDefaultAggregate(aggregateFunction) : increment;
  }
}
