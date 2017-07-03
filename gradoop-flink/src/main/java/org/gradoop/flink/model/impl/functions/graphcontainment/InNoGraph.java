
package org.gradoop.flink.model.impl.functions.graphcontainment;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.api.entities.EPGMGraphElement;

/**
 * True, if the element has not graph ids.
 *
 * @param <EL> epgm graph element
 */
public class InNoGraph<EL extends EPGMGraphElement> implements FilterFunction<EL> {

  @Override
  public boolean filter(EL value) throws Exception {
    return value.getGraphIds() == null || value.getGraphIds().isEmpty();
  }
}
