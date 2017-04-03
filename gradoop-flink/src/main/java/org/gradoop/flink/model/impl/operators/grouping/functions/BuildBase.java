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

package org.gradoop.flink.model.impl.operators.grouping.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation
  .CountAggregator;
import org.gradoop.common.model.api.entities.EPGMAttributed;
import org.gradoop.common.model.api.entities.EPGMLabeled;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueList;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
   * Property keys that are used for grouping.
   */
  private final List<String> groupPropertyKeys;

  /**
   * True, if the label shall be considered.
   */
  private final boolean useLabel;

  /**
   * Aggregate functions that are applied on grouped elements.
   */
  private final List<PropertyValueAggregator> valueAggregators;

  /**
   * Stores all aggregator property keys for each label.
   */
  private final Map<String, Set<String>> labelWithAggregatorPropertyKeys;

  /**
   * Creates build base.
   *
   * @param groupPropertyKeys               property keys used for grouping
   * @param useLabel                        true, if element label shall be used for grouping
   * @param valueAggregators                aggregate functions for super elements
   * @param labelWithAggregatorPropertyKeys stores all aggregator property keys for each label
   */
  protected BuildBase(List<String> groupPropertyKeys,
    boolean useLabel, List<PropertyValueAggregator> valueAggregators,
    Map<String, Set<String>> labelWithAggregatorPropertyKeys) {
    this.groupPropertyKeys  = groupPropertyKeys;
    this.useLabel           = useLabel;
    this.valueAggregators   = valueAggregators;
    this.labelWithAggregatorPropertyKeys = labelWithAggregatorPropertyKeys;
  }

  /**
   * Resets the underlying aggregators
   */
  protected void resetAggregators() {
    if (doAggregate()) {
      for (PropertyValueAggregator valueAggregator : valueAggregators) {
        valueAggregator.resetAggregate();
      }
    }
  }

  /**
   * Returns a map which stores all aggregator property keys for each label.
   *
   * @return aggregator key to element labels map
   */
  protected Map<String, Set<String>> getLabelWithAggregatorPropertyKeys() {
    return labelWithAggregatorPropertyKeys;
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

  /**
   * Returns the label or {@code null} if {@link #useLabel()} is {@code false}.
   *
   * @param labeled labeled element
   * @return label or {@code null}
   */
  protected String getLabel(EPGMLabeled labeled) {
    return useLabel() ? labeled.getLabel() : null;
  }

  /**
   * Sets the given label if {@link #useLabel()} returns {@code true}.
   *
   * @param labeled labeled element
   * @param label   group label
   */
  protected void setLabel(EPGMLabeled labeled, String label) {
    if (useLabel()) {
      labeled.setLabel(label);
    }
  }

  //----------------------------------------------------------------------------
  // Grouping properties
  //----------------------------------------------------------------------------

  /**
   * Returns a {@link PropertyValueList} containing all grouping values. If an
   * element does not have a value for a specific key, the corresponding value
   * is set to {@code PropertyValue.NULL_VALUE}.
   *
   * @param attributed EPGM attributed element
   * @return property value list
   */
  protected PropertyValueList getGroupProperties(EPGMAttributed attributed)
      throws IOException {
    List<PropertyValue> values =
      Lists.newArrayListWithCapacity(attributed.getPropertyCount());

    for (String groupPropertyKey : groupPropertyKeys) {
      if (attributed.hasProperty(groupPropertyKey)) {
        values.add(attributed.getPropertyValue(groupPropertyKey));
      } else {
        values.add(PropertyValue.NULL_VALUE);
      }
    }

    return PropertyValueList.fromPropertyValues(values);
  }

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

  /**
   * Returns the property keys used for Grouping.
   *
   * @return group property keys
   */
  protected List<String> getGroupPropertyKeys() {
    return groupPropertyKeys;
  }

  //----------------------------------------------------------------------------
  // Aggregation
  //----------------------------------------------------------------------------

  /**
   * Returns true, if the group shall be aggregated.
   *
   * @return true, iff the group shall be aggregated
   */
  protected boolean doAggregate() {
    return !valueAggregators.isEmpty();
  }

  /**
   * Returns the property values of the given element which are used for
   * aggregation. If the EPGM element does not have a property, it uses
   * {@code PropertyValue.NULL_VALUE} instead.
   *
   * @param   element attributed EPGM element
   * @return  property values for aggregation
   */
  protected PropertyValueList getAggregateValues(EPGMElement element)
    throws IOException {
    List<PropertyValue> propertyValues = Lists.newArrayList();

    for (PropertyValueAggregator valueAggregator : valueAggregators) {
      // check if the aggregator is used for the elements label
      if (verifyAggregatorForLabel(valueAggregator.getAggregatePropertyKey(), element.getLabel()) ||
        // or used for all vertices
        verifyAggregatorForLabel(
          valueAggregator.getAggregatePropertyKey(), Grouping.VERTEX_AGGREGATOR) ||
        // or used for all edges
        verifyAggregatorForLabel(
          valueAggregator.getAggregatePropertyKey(), Grouping.EDGE_AGGREGATOR)) {
        String propertyKey = valueAggregator.getPropertyKey();
        if (valueAggregator instanceof CountAggregator) {
          propertyValues.add(ONE);
        } else if (element.hasProperty(propertyKey)) {
          propertyValues.add(element.getPropertyValue(propertyKey));
        } else {
          propertyValues.add(PropertyValue.NULL_VALUE);
        }
      }
    }
    return PropertyValueList.fromPropertyValues(propertyValues);
  }

  /**
   * Add the given values to the corresponding aggregate.
   *
   * @param label element label
   * @param values property values
   */
  protected void aggregate(String label, PropertyValueList values) {
    Iterator<PropertyValue> valueIt = values.iterator();

    for (PropertyValueAggregator valueAggregator : valueAggregators) {
      // check if the aggregator is used for the elements label
      if (verifyAggregatorForLabel(valueAggregator.getAggregatePropertyKey(), label) ||
        // or used for all vertices
        verifyAggregatorForLabel(
          valueAggregator.getAggregatePropertyKey(), Grouping.VERTEX_AGGREGATOR) ||
        // or used for all edges
        verifyAggregatorForLabel(
          valueAggregator.getAggregatePropertyKey(), Grouping.EDGE_AGGREGATOR)) {
        PropertyValue value = valueIt.next();
        valueAggregator.aggregate(value);
      }
    }
  }

  /**
   * Returns the current aggregate values from the aggregators.
   *
   * @return aggregate values
   */
  protected PropertyValueList getAggregateValues() throws IOException {
    PropertyValueList result;
    if (!doAggregate()) {
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
   * @param label   element label
   */
  protected void setAggregateValues(EPGMAttributed element, String label) {
    if (doAggregate()) {
      for (PropertyValueAggregator valueAggregator : valueAggregators) {
        // check if the aggregator is used for the elements label
        if (verifyAggregatorForLabel(valueAggregator.getAggregatePropertyKey(), label) ||
          // or used for all edges
          verifyAggregatorForLabel(
            valueAggregator.getAggregatePropertyKey(), Grouping.EDGE_AGGREGATOR)) {
          element.setProperty(
            valueAggregator.getAggregatePropertyKey(),
            valueAggregator.getAggregate());
        }
      }
    }
  }

  /**
   * Sets the given property values as new properties at the given element.
   *
   * @param element attributed element
   * @param label element label
   * @param values aggregate values
   */
  protected void setAggregateValues(
    EPGMAttributed element, String label, PropertyValueList values) {
    if (doAggregate()) {
      Iterator<PropertyValue> valueIt = values.iterator();

      for (PropertyValueAggregator valueAggregator : valueAggregators) {
        // check if the aggregator is used for the elements label
        if (verifyAggregatorForLabel(valueAggregator.getAggregatePropertyKey(), label) ||
          // or used for all vertices
          verifyAggregatorForLabel(
            valueAggregator.getAggregatePropertyKey(), Grouping.VERTEX_AGGREGATOR)) {
          PropertyValue value = valueIt.next();
          element.setProperty(
            valueAggregator.getAggregatePropertyKey(),
            value);
        }
      }
    }
  }

  /**
   * Verifies if the given aggregator property key shall be used fo the elements label.
   *
   * @param aggregatorPropertyKey aggregator property key
   * @param elementLabel element label
   * @return true if the aggregator shall be used for the element
   */
  private boolean verifyAggregatorForLabel(String aggregatorPropertyKey, String elementLabel) {
    return labelWithAggregatorPropertyKeys
      .getOrDefault(aggregatorPropertyKey, Sets.newHashSet())
      .contains(elementLabel);
  }
}
