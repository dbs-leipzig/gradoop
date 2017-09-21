package org.gradoop.flink.algorithms.gelly.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Convert a Gradoop {@link Vertex} to a Gelly Vertex with a constant or a {@link PropertyValue} as its value.
 *
 * @param <E> Type of the output Gelly Vertex.
 */
public class VertexToGellyVertex<E> implements MapFunction<Vertex, org.apache.flink.graph.Vertex<GradoopId, E>> {

  /**
   * Should the value be read from the Vertex Properties?
   */
  private final boolean useProperty;

  /**
   * Value to use as a default.
   */
  private final E defaultValue;

  /**
   * Key of the Property to use.
   */
  private final String propertyKey;

  /**
   * Constructor for the map function extracting the value as a {@link PropertyValue}.
   *
   * @param propertyKey Key of the Property.
   */
  public VertexToGellyVertex(String propertyKey) {
    this.useProperty = true;
    this.defaultValue = null;
    this.propertyKey = propertyKey;
  }

  /**
   * Constructor for the map function using a constant default value.
   *
   * @param defaultValue The value.
   */
  public VertexToGellyVertex(E defaultValue) {
    this.useProperty = false;
    this.defaultValue = defaultValue;
    this.propertyKey = null;
  }

  @Override
  public org.apache.flink.graph.Vertex<GradoopId, E> map(Vertex vertex) {
    E value;
    if (useProperty) {
      value = (E) vertex.getPropertyValue(propertyKey);
    } else {
      value = defaultValue;
    }
    return new org.apache.flink.graph.Vertex<>(vertex.getId(), value);
  }
}
