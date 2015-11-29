package org.gradoop.storage.api;

import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.impl.id.GradoopIdSet;

import java.io.Serializable;

/**
 * Base interface for creating persistent graph data from transient graph data.
 *
 * @param <G>   EPGM graph head type
 * @param <PG>  Persistent graph head type
 */
public interface PersistentGraphHeadFactory<
  G extends EPGMGraphHead,
  PG extends PersistentGraphHead>
  extends Serializable {

  /**
   * Creates graph data based on the given parameters.
   *
   * @param inputGraphData input graph data
   * @param vertices       vertices contained in that graph
   * @param edges          edges contained in that graph
   * @return graph data
   */
  PG createGraphHead(
    G inputGraphData, GradoopIdSet vertices, GradoopIdSet edges);
}
