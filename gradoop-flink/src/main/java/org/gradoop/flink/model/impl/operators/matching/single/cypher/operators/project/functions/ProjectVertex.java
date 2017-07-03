
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.project.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingFactory;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;

import java.util.List;

/**
 * Projects a Vertex by a set of properties.
 * Vertex -> Embedding(GraphElementEmbedding(Vertex))
 */
public class ProjectVertex extends RichMapFunction<Vertex, Embedding> {
  /**
   * Names of the properties that will be kept in the projection
   */
  private final List<String> propertyKeys;

  /**
   * Creates a new vertex projection function
   * @param propertyKeys List of propertyKeys that will be kept in the projection
   */
  public ProjectVertex(List<String> propertyKeys) {
    this.propertyKeys = propertyKeys;
  }

  @Override
  public Embedding map(Vertex vertex) {
    return EmbeddingFactory.fromVertex(vertex, propertyKeys);
  }
}
