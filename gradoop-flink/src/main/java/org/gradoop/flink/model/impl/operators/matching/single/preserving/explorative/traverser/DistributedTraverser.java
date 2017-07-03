
package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;

import java.util.Objects;

/**
 * A distributed traverser extracts embeddings from a given graph.
 *
 * @param <K> key type
 */
public abstract class DistributedTraverser<K> {
  /**
   * Strategy for vertex and edge mappings
   */
  private final MatchStrategy matchStrategy;
  /**
   * Controls the graph traversal
   */
  private final TraversalCode traversalCode;
  /**
   * Number of vertices in the query graph.
   */
  private final int vertexCount;
  /**
   * Number of edges in the query graph.
   */
  private final int edgeCount;
  /**
   * Needed to build initial embeddings
   */
  private final Class<K> keyClazz;
  /**
   * Vertex mapping used for debug
   */
  private final DataSet<Tuple2<K, PropertyValue>> vertexMapping;
  /**
   * Edge mapping used for debug
   */
  private final DataSet<Tuple2<K, PropertyValue>> edgeMapping;

  /**
   * Creates a new distributed traverser.
   *
   * @param traversalCode          describes the graph traversal
   * @param matchStrategy          matching strategy for vertices and edges
   * @param vertexCount            number of query vertices
   * @param edgeCount              number of query edges
   * @param keyClazz               key type for embedding initialization
   * @param vertexMapping          used for debug
   * @param edgeMapping            used for debug
   */
  DistributedTraverser(TraversalCode traversalCode,
    MatchStrategy matchStrategy,
    int vertexCount, int edgeCount,
    Class<K> keyClazz,
    DataSet<Tuple2<K, PropertyValue>> vertexMapping,
    DataSet<Tuple2<K, PropertyValue>> edgeMapping) {

    Objects.requireNonNull(traversalCode);
    Objects.requireNonNull(matchStrategy);
    Objects.requireNonNull(keyClazz);

    this.traversalCode          = traversalCode;
    this.matchStrategy          = matchStrategy;
    this.vertexCount            = vertexCount;
    this.edgeCount              = edgeCount;
    this.keyClazz               = keyClazz;
    this.vertexMapping          = vertexMapping;
    this.edgeMapping            = edgeMapping;
  }

  int getVertexCount() {
    return vertexCount;
  }

  int getEdgeCount() {
    return edgeCount;
  }

  Class<K> getKeyClazz() {
    return keyClazz;
  }

  TraversalCode getTraversalCode() {
    return traversalCode;
  }

  MatchStrategy getMatchStrategy() {
    return matchStrategy;
  }

  DataSet<Tuple2<K, PropertyValue>> getVertexMapping() {
    return vertexMapping;
  }

  DataSet<Tuple2<K, PropertyValue>> getEdgeMapping() {
    return edgeMapping;
  }

  /**
   * True, if the traverser runs in a bulk or delta iteration.
   *
   * @return true, if traverser is using Flink bulk or delta iteration
   */
  abstract boolean isIterative();
}
