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
package org.gradoop.flink.model.impl.operators.grouping.functions;

import com.google.common.collect.Lists;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.CountAggregator;
import org.gradoop.common.model.api.entities.EPGMAttributed;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueList;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * Encapsulates logic that is used for building summarized vertices and edges.
 */
abstract class BuildBase implements Serializable {

  /**
   * Class version for serialization.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Used for count aggregation.
   */
  private static final PropertyValue ONE = PropertyValue.create(1L);

  /**
   * True, if the label shall be considered.
   */
  private final boolean useLabel;

  /**
   * Creates build base.
   *
   * @param useLabel    use edge label
   */
  protected BuildBase(boolean useLabel) {
    this.useLabel = useLabel;

  }

  /**
   * Resets the underlying aggregators
   *
   * @param valueAggregators aggregate functions to be reset
   */
  protected void resetAggregators(List<PropertyValueAggregator> valueAggregators) {
    for (PropertyValueAggregator valueAggregator : valueAggregators) {
      valueAggregator.resetAggregate();
    }
  }

  //----------------------------------------------------------------------------
  // Label
  //----------------------------------------------------------------------------

  /**
   * Returns true, if the label of the element shall be considered during
   * summarization.
   *
   * @return true, iff the element label shall be considered
   */
  protected boolean useLabel() {
    return useLabel;
  }

  //----------------------------------------------------------------------------
  // Grouping properties
  //----------------------------------------------------------------------------

  /**
   * Adds the given group properties to the attributed element.
   *
   * @param attributed          attributed element
   * @param groupPropertyValues group property values
   * @param vertexLabelGroup    vertex label group
   */
  protected void setGroupProperties(EPGMAttributed attributed,
    PropertyValueList groupPropertyValues, LabelGroup vertexLabelGroup) {

    Iterator<PropertyValue> valueIterator = groupPropertyValues.iterator();

    for (String groupPropertyKey : vertexLabelGroup.getPropertyKeys()) {
      attributed.setProperty(groupPropertyKey, valueIterator.next());
    }
  }

  //----------------------------------------------------------------------------
  // Aggregation
  //----------------------------------------------------------------------------

  /**
   * Returns true, if the group shall be aggregated.
   *
   * @param   valueAggregators aggregate functions
   * @return  true, iff the group shall be aggregated
   */
  protected boolean doAggregate(List<PropertyValueAggregator> valueAggregators) {
    return !valueAggregators.isEmpty();
  }

  /**
   * Returns the property values of the given element which are used for
   * aggregation. If the EPGM element does not have a property, it uses
   * {@code PropertyValue.NULL_VALUE} instead.
   *
   * @param   element           attributed EPGM element
   * @param   valueAggregators  aggregate functions
   * @return  property values for aggregation
   */
  protected PropertyValueList getAggregateValues(
    EPGMElement element, List<PropertyValueAggregator> valueAggregators) throws IOException {
    List<PropertyValue> propertyValues = Lists.newArrayList();
    String propertyKey;

    for (PropertyValueAggregator valueAggregator : valueAggregators) {
      propertyKey = valueAggregator.getPropertyKey();
      if (valueAggregator instanceof CountAggregator) {
        propertyValues.add(ONE);
      } else if (element.hasProperty(propertyKey)) {
        propertyValues.add(element.getPropertyValue(propertyKey));
      } else {
        propertyValues.add(PropertyValue.NULL_VALUE);
      }
    }

    return PropertyValueList.fromPropertyValues(propertyValues);
  }

  /**
   * Add the given values to the corresponding aggregate.
   *
   * @param values property values
   * @param valueAggregators aggregate functions
   */
  protected void aggregate(
    PropertyValueList values, List<PropertyValueAggregator> valueAggregators) {
    Iterator<PropertyValue> valueIt = values.iterator();
    PropertyValue value;

    for (PropertyValueAggregator valueAggregator : valueAggregators) {
      value = valueIt.next();
      valueAggregator.aggregate(value);
    }
  }

  /**
   * Returns the current aggregate values from the aggregators.
   *
   * @param valueAggregators aggregate functions
   * @return aggregate values
   */
  protected PropertyValueList getAggregateValues(List<PropertyValueAggregator> valueAggregators)
    throws IOException {
    PropertyValueList result;
    if (!doAggregate(valueAggregators)) {
      result = PropertyValueList.createEmptyList();
    } else {
      List<PropertyValue> propertyValues =
        Lists.newArrayListWithCapacity(valueAggregators.size());
      for (PropertyValueAggregator valueAggregator : valueAggregators) {
        propertyValues.add(valueAggregator.getAggregate());
      }
      result = PropertyValueList.fromPropertyValues(propertyValues);
    }
    return result;
  }

  /**
   * Sets the final aggregate value as a new property at the given element. The
   * values are fetched from the internal aggregators.
   *
   * @param element attributed element
   * @param valueAggregators aggregate functions
   */
  protected void setAggregateValues(
    EPGMAttributed element, List<PropertyValueAggregator> valueAggregators) {
    if (doAggregate(valueAggregators)) {
      for (PropertyValueAggregator valueAggregator : valueAggregators) {
        element.setProperty(
          valueAggregator.getAggregatePropertyKey(),
          valueAggregator.getAggregate());
      }
    }
  }

  /**
   * Sets the given property values as new properties at the given element.
   *
   * @param element attributed element
   * @param values aggregate values
   * @param valueAggregators aggregate functions
   */
  protected void setAggregateValues(
    EPGMAttributed element,
    PropertyValueList values,
    List<PropertyValueAggregator> valueAggregators) {
    if (doAggregate(valueAggregators)) {
      Iterator<PropertyValue> valueIt = values.iterator();

      for (PropertyValueAggregator valueAggregator : valueAggregators) {
        PropertyValue value = valueIt.next();
        element.setProperty(valueAggregator.getAggregatePropertyKey(), value);
      }
    }
  }
}
