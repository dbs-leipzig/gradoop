
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Filters elements if their identifier is equal to the given identifier.
 *
 * @param <EL> EPGM element type
 */
@FunctionAnnotation.ReadFields("id")
public class BySameId<EL extends Element> implements FilterFunction<EL> {

  /**
   * id
   */
  private final GradoopId id;

  /**
   * Creates a new filter instance
   *
   * @param id identifier
   */
  public BySameId(GradoopId id) {
    this.id = id;
  }

  @Override
  public boolean filter(EL element) throws Exception {
    return element.getId().equals(id);
  }
}
