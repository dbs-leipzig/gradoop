
package org.gradoop.common.model.impl.pojo;

import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * POJO Implementation of an EPGM vertex.
 */
public class Vertex extends GraphElement implements EPGMVertex {

  /**
   * Default constructor.
   */
  public Vertex() {
  }

  /**
   * Creates a vertex based on the given parameters.
   *
   * @param id         vertex identifier
   * @param label      vertex label
   * @param properties vertex properties
   * @param graphs     graphs that vertex is contained in
   */
  public Vertex(final GradoopId id, final String label,
    final Properties properties, final GradoopIdList graphs) {
    super(id, label, properties, graphs);
  }

  @Override
  public String toString() {
    return String.format("(%s)", super.toString());
  }
}
