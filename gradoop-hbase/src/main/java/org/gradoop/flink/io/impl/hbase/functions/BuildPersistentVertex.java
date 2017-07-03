
package org.gradoop.flink.io.impl.hbase.functions;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.storage.api.PersistentVertex;
import org.gradoop.common.storage.api.PersistentVertexFactory;

import java.util.Set;

/**
 * Used to create persistent vertex data from vertex data and
 * outgoing/incoming edge data.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class BuildPersistentVertex<V extends EPGMVertex, E extends EPGMEdge>
  implements CoGroupFunction
  <Tuple2<V, Set<E>>, Tuple2<GradoopId, Set<E>>, PersistentVertex<E>> {

  /**
   * Persistent vertex data factory.
   */
  private final PersistentVertexFactory<V, E> vertexFactory;

  /**
   * Creates co group function.
   *
   * @param vertexFactory persistent vertex data factory
   */
  public BuildPersistentVertex(PersistentVertexFactory<V, E> vertexFactory) {
    this.vertexFactory = vertexFactory;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void coGroup(Iterable<Tuple2<V, Set<E>>> iterable,
    Iterable<Tuple2<GradoopId, Set<E>>> iterable1,
    Collector<PersistentVertex<E>> collector) throws Exception {
    V vertex = null;
    Set<E> outgoingEdgeData = null;
    Set<E> incomingEdgeData = null;
    for (Tuple2<V, Set<E>> left : iterable) {
      vertex = left.f0;
      outgoingEdgeData = left.f1;
    }
    for (Tuple2<GradoopId, Set<E>> right : iterable1) {
      incomingEdgeData = right.f1;
    }
    assert vertex != null;
    collector.collect(vertexFactory
      .createVertex(vertex, outgoingEdgeData, incomingEdgeData));
  }
}
