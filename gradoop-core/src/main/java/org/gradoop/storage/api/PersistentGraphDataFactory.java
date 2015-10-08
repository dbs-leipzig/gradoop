package org.gradoop.storage.api;

import org.gradoop.model.api.GraphData;

import java.io.Serializable;
import java.util.Set;

/**
 * Base interface for creating persistent graph data from transient graph data.
 *
 * @param <IGD> input graph data type
 * @param <OGD> output graph data type
 */
public interface PersistentGraphDataFactory<IGD extends GraphData, OGD
  extends PersistentGraphData> extends
  Serializable {

  /**
   * Creates graph data based on the given parameters.
   *
   * @param inputGraphData input graph data
   * @param vertices       vertices contained in that graph
   * @param edges          edges contained in that graph
   * @return graph data
   */
  OGD createGraphData(IGD inputGraphData, Set<Long> vertices, Set<Long> edges);
}
