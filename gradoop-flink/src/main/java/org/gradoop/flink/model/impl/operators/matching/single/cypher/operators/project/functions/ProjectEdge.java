
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.project.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingFactory;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;

import java.util.List;

/**
 * Projects an Edge by a set of properties.
 * Edge -> Embedding(GraphElementEmbedding(Edge))
 */
public class ProjectEdge extends RichMapFunction<Edge, Embedding> {
  /**
   * Names of the properties that will be kept in the projection
   */
  private final List<String> propertyKeys;
  /**
   * Indicates if the edges is a loop
   */
  private final boolean isLoop;


  /**
   * Creates a new edge projection function
   * @param propertyKeys the property keys that will be kept
   * @param isLoop indicates if edges is a loop
   */
  public ProjectEdge(List<String> propertyKeys, boolean isLoop) {
    this.propertyKeys = propertyKeys;
    this.isLoop = isLoop;
  }

  @Override
  public Embedding map(Edge edge) {
    return EmbeddingFactory.fromEdge(edge, propertyKeys, isLoop);
  }
}
