
package org.gradoop.common.storage.impl.hbase;

import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.storage.api.PersistentGraphHeadFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default factory for creating persistent graph data representation.
 *
 * @param <G> EPGM graph head type
 */
public class HBaseGraphHeadFactory<G extends EPGMGraphHead>
  implements PersistentGraphHeadFactory<G> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * {@inheritDoc}
   */
  @Override
  public HBaseGraphHead<G> createGraphHead(G inputGraphHead,
    GradoopIdList vertices, GradoopIdList edges) {
    checkNotNull(inputGraphHead, "EPGMGraphHead was null");
    checkNotNull(vertices, "EPGMVertex identifiers were null");
    checkNotNull(edges, "EPGMEdge identifiers were null");
    return new HBaseGraphHead<>(inputGraphHead, vertices, edges);
  }
}
