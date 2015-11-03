package org.gradoop.storage.api;

import org.gradoop.model.api.EPGMGraphHead;

import java.io.Serializable;
import java.util.Set;

/**
 * Base interface for creating persistent graph data from transient graph data.
 *
 * @param <IGD> input graph data type
 * @param <OGD> output graph data type
 */
public interface PersistentGraphHeadFactory<IGD extends EPGMGraphHead, OGD
  extends PersistentGraphHead> extends
  Serializable {

  /**
   * Creates graph data based on the given parameters.
   *
   * @param inputGraphData input graph data
   * @param vertices       vertices contained in that graph
   * @param edges          edges contained in that graph
   * @return graph data
   */
  OGD createGraphHead(IGD inputGraphData, Set<Long> vertices, Set<Long> edges);
}
