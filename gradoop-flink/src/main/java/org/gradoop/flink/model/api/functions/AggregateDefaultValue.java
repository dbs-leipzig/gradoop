
package org.gradoop.flink.model.api.functions;

import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Describes an extension of an {@link AggregateFunction}, in the case there
 * is a logical default, e.g., when counting specific vertex labels and there
 * is no such vertex than one can specify 0 which will be returned instead of
 * NULL.
 */
public interface AggregateDefaultValue {

  /**
   * Defines the default value.
   *
   * @return default value.
   */
  PropertyValue getDefaultValue();
}
