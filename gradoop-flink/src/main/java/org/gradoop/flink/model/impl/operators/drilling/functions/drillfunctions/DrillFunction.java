package org.gradoop.flink.model.impl.operators.drilling.functions.drillfunctions;

import org.gradoop.common.model.impl.properties.PropertyValue;

import java.io.Serializable;

/**
 * Interface for all drill functions which are used for drill down / roll up operations.
 */
public interface DrillFunction extends Serializable {

  /**
   * Returns a changed property value based in the property before drilling.
   *
   * @param property current property
   * @return property after drilling
   */
  PropertyValue execute(PropertyValue property);

}
