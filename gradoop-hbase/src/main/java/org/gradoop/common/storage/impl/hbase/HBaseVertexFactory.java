
package org.gradoop.common.storage.impl.hbase;

import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.storage.api.PersistentVertexFactory;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default factory for creating persistent vertex data representation.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class HBaseVertexFactory<V extends EPGMVertex, E extends EPGMEdge>
  implements PersistentVertexFactory<V, E> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * {@inheritDoc}
   */
  @Override
  public HBaseVertex<V, E> createVertex(V inputVertex,
    Set<E> outgoingEdges, Set<E> incomingEdges) {
    checkNotNull(inputVertex, "Input vertex was null");
    return new HBaseVertex<>(inputVertex, outgoingEdges, incomingEdges);
  }
}
