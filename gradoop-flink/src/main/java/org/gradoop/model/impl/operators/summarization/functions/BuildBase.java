package org.gradoop.model.impl.operators.summarization.functions;

import com.google.common.collect.Lists;
import org.gradoop.model.api.EPGMAttributed;
import org.gradoop.model.api.EPGMLabeled;
import org.gradoop.model.impl.operators.summarization.functions.aggregation.PropertyValueAggregator;
import org.gradoop.model.impl.properties.PropertyValue;
import org.gradoop.model.impl.properties.PropertyValueList;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * Encapsulates logic that is used for building summarized vertices and edges.
 */
public abstract class BuildBase implements Serializable {

  /**
   * Class version for serialization.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Property keys that are used for grouping.
   */
  private final List<String> groupPropertyKeys;

  /**
   * True, if the label shall be considered.
   */
  private final boolean useLabel;

  /**
   * Aggregate function that is applied on grouped elements.
   */
  private final PropertyValueAggregator valueAggregator;

  /**
   * Creates build base.
   *
   * @param groupPropertyKeys property keys used for grouping
   * @param useLabel          true, if element label shall be used for grouping
   * @param valueAggregator   aggregate function for element values
   */
  protected BuildBase(List<String> groupPropertyKeys,
    boolean useLabel, PropertyValueAggregator valueAggregator) {
    this.groupPropertyKeys  = groupPropertyKeys;
    this.useLabel           = useLabel;
    this.valueAggregator    = valueAggregator;
  }

  /**
   * Resets the underlying aggregator
   */
  protected void resetAggregator() {
    if (doAggregate()) {
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
   */
  protected void setGroupProperties(EPGMAttributed attributed,
    PropertyValueList groupPropertyValues) {

    Iterator<String> keyIterator = groupPropertyKeys.iterator();
    Iterator<PropertyValue> valueIterator = groupPropertyValues.iterator();

    while (keyIterator.hasNext() && valueIterator.hasNext()) {
      attributed.setProperty(keyIterator.next(), valueIterator.next());
    }
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
    return valueAggregator != null;
  }

  /**
   * Returns the property value of the given element which is used for the
   * aggregation. If the element does not have the property, the method returns
   * {@code PropertyValue.NULL_VALUE}.
   *
   * @param   attributed attributed element
   * @return  property value for aggregate or {@code PropertyValue.NULL_VALUE}
   *          if property not present
   */
  protected PropertyValue getValueForAggregation(EPGMAttributed attributed) {
    PropertyValue result;
    String propertyKey = valueAggregator.getPropertyKey();
    if (attributed.hasProperty(propertyKey)) {
      result = attributed.getPropertyValue(propertyKey);
    } else {
      result = PropertyValue.NULL_VALUE;
    }
    return result;
  }

  /**
   * Add the given value to the aggregate.
   *
   * @param value property value
   */
  protected void aggregate(PropertyValue value) {
    valueAggregator.aggregate(value);
  }

  /**
   * Returns the current aggregate value.
   *
   * @return aggregate value or {@code PropertyValue.NULL_VALUE}
   */
  protected PropertyValue getAggregate() {
    return doAggregate() ?
      valueAggregator.getAggregate() : PropertyValue.NULL_VALUE;
  }

  /**
   * Sets the final aggregate value as a new property at the given element. The
   * value is fetched from the internal aggregator.
   *
   * @param element attributed element
   */
  protected void setAggregate(EPGMAttributed element) {
    if (doAggregate()) {
      element.setProperty(
        valueAggregator.getAggregatePropertyKey(),
        valueAggregator.getAggregate());
    }
  }

  /**
   * Sets the given value as a new property at the given element.
   *
   * @param element attributed element
   * @param value   final aggregate value
   */
  protected void setAggregate(EPGMAttributed element,
    PropertyValue value) {
    element.setProperty(valueAggregator.getAggregatePropertyKey(), value);
  }
}
