
package org.gradoop.common.model.impl.pojo;

import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * POJO Implementation of an EPGM graph head.
 */
public class GraphHead extends Element implements EPGMGraphHead {

  /**
   * Default constructor.
   */
  public GraphHead() {
  }

  /**
   * Creates a graph head based on the given parameters.
   *
   * @param id         graph identifier
   * @param label      graph label
   * @param properties graph properties
   */
  public GraphHead(final GradoopId id, final String label,
    final Properties properties) {
    super(id, label, properties);
  }

  @Override
  public String toString() {
    return super.toString() + "[]";
  }
}
