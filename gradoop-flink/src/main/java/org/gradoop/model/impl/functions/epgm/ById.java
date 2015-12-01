package org.gradoop.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.model.api.EPGMElement;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Filters elements by id.
 *
 * @param <EL> element type
 */
public class ById<EL extends EPGMElement> implements FilterFunction<EL> {

  /**
   * id
   */
  private final GradoopId id;

  /**
   * constructor
   * @param id id
   */
  public ById(GradoopId id) {
    this.id = id;
  }

  @Override
  public boolean filter(EL element) throws Exception {
    return element.getId().equals(id);
  }
}
