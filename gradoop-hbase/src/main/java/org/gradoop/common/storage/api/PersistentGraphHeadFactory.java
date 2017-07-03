
package org.gradoop.common.storage.api;

import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.impl.id.GradoopIdList;

import java.io.Serializable;

/**
 * Base interface for creating persistent graph data from transient graph data.
 *
 * @param <G> EPGM graph head type
 */
public interface PersistentGraphHeadFactory<G extends EPGMGraphHead>
  extends Serializable {

  /**
   * Creates graph data based on the given parameters.
   *
   * @param inputGraphData input graph data
   * @param vertices       vertices contained in that graph
   * @param edges          edges contained in that graph
   * @return graph data
   */
  PersistentGraphHead createGraphHead(G inputGraphData,
    GradoopIdList vertices, GradoopIdList edges);
}
