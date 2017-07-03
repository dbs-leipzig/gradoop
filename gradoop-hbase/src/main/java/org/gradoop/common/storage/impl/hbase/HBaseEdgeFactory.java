
package org.gradoop.common.storage.impl.hbase;

import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.storage.api.PersistentEdgeFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default factory for creating persistent edge representations.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class HBaseEdgeFactory<E extends EPGMEdge, V extends EPGMVertex>
  implements PersistentEdgeFactory<E, V> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * {@inheritDoc}
   */
  @Override
  public HBaseEdge<E, V> createEdge(E inputEdge, V sourceVertex, V
    targetVertex) {
    checkNotNull(inputEdge, "EPGMEdge was null");
    checkNotNull(sourceVertex, "Source vertex was null");
    checkNotNull(targetVertex, "Target vertex was null");
    return new HBaseEdge<>(inputEdge, sourceVertex, targetVertex);
  }
}
