package org.gradoop.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.api.EPGMElement;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Clones an element by replacing its id but keeping label and properties.
 *
 * @param <EL> element type
 */
public class Clone<EL extends EPGMElement> implements MapFunction<EL, EL> {

  @Override
  public EL map(EL el) throws Exception {
    el.setId(GradoopId.get());
    return el;
  }
}
