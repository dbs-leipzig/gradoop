package org.gradoop.model.impl.algorithms.btg.pojos;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.gradoop.model.impl.algorithms.btg.utils.BTGVertexType;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Custom vertex used by
 * {@link org.gradoop.model.impl.algorithms.btg.BTGAlgorithm}.
 */
public class BTGVertexValue {
  /**
   * The vertex type.
   */
  private BTGVertexType vertexType;
  /**
   * The value of that vertex.
   */
  private Double vertexValue;
  /**
   * The list of BTGs that vertex belongs to.
   */
  private List<Long> btgIDs;
  /**
   * Stores the minimum vertex ID per message sender. This is only used by
   * vertices of type {@link BTGVertexType} MASTER, so it is only
   * initialized
   * when needed.
   */
  private Map<Long, Long> neighborMinimumBTGIds;

  /**
   * Initializes an IIGVertex based on the given parameters.
   *
   * @param vertexType  The type of that vertex
   * @param vertexValue The value stored at that vertex
   * @param btgIDs      A list of BTGs that vertex belongs to
   */
  public BTGVertexValue(BTGVertexType vertexType, Double vertexValue,
    List<Long> btgIDs) {
    this.vertexType = vertexType;
    this.vertexValue = vertexValue;
    this.btgIDs = btgIDs;
  }

  /**
   * Returns the type of that vertex.
   *
   * @return vertex type
   */
  public BTGVertexType getVertexType() {
    return this.vertexType;
  }

  /**
   * Returns the value of that vertex.
   *
   * @return vertex value
   */
  public Double getVertexValue() {
    return this.vertexValue;
  }

  /**
   * Sets the value of that vertex.
   *
   * @param vertexValue value to be set
   */
  public void setVertexValue(Double vertexValue) {
    this.vertexValue = vertexValue;
  }

  /**
   * Returns the list of BTGs that vertex belongs to.
   *
   * @return list of BTGs containing that vertex.
   */
  public Iterable<Long> getGraphs() {
    return this.btgIDs;
  }

  /**
   * Adds a BTG to the list of BTGs. BTGs can occur multiple times.
   *
   * @param graph BTG ID to be added
   */
  public void addGraph(Long graph) {
    if (this.btgIDs.isEmpty()) {
      resetGraphs();
    }
    this.btgIDs.add(graph);
  }

  /**
   * Adds BTGs to the list of BTGs. BTGs can occur multiple times.
   *
   * @param graphs BTG IDs to be added
   */
  public void addGraphs(Iterable<Long> graphs) {
    if (this.btgIDs.isEmpty()) {
      resetGraphs();
    }
    for (Long btg : graphs) {
      this.btgIDs.add(btg);
    }
  }

  /**
   * Resets the list of BTGs
   */
  public void resetGraphs() {
    this.btgIDs = Lists.newArrayList();
  }

  /**
   * Returns the size of the BTGs
   *
   * @return actual size of BTG List
   */
  public int getGraphCount() {
    return this.btgIDs.size();
  }

  /**
   * Returned the last inserted BTG ID. This has become necessary for the
   * Flink-Messaging-Function
   *
   * @return last added BTG ID
   */
  public Long getLastGraph() {
    if (this.btgIDs.size() > 0) {
      return btgIDs.get(btgIDs.size() - 1);
    }
    return null;
  }

  /**
   * Removes the last inserted BTG ID. This is necessary for non-master vertices
   * as they need to store only the minimum BTG ID, because they must only occur
   * in one BTG.
   */
  public void removeLastBtgID() {
    if (this.btgIDs.size() > 0) {
      this.btgIDs.remove(this.btgIDs.size() - 1);
    }
  }

  /**
   * Stores the given map between vertex id and BTG id if the pair does not
   * exist. It it exists, the BTG id is updated iff it is smaller than the
   * currently stored BTG id.
   *
   * @param vertexID vertex id of a neighbour node
   * @param btgID    BTG id associated with the neighbour node
   */
  public void updateNeighbourBtgID(Long vertexID, Long btgID) {
    if (neighborMinimumBTGIds == null) {
      initNeighbourMinimBTGIDMap();
    }
    if (!neighborMinimumBTGIds.containsKey(vertexID) ||
      (neighborMinimumBTGIds.containsKey(vertexID) &&
        neighborMinimumBTGIds.get(vertexID) > btgID)) {
      neighborMinimumBTGIds.put(vertexID, btgID);
    }
  }

  /**
   * Updates the set of BTG ids this vertex is involved in according to the set
   * of minimum values stored in the mapping between neighbour nodes and BTG
   * ids. This is only necessary for master data nodes like described in
   */
  public void updateBtgIDs() {
    if (this.neighborMinimumBTGIds != null) {
      Set<Long> newBtgIDs = new HashSet<>();
      for (Map.Entry<Long, Long> e : this.neighborMinimumBTGIds.entrySet()) {
        newBtgIDs.add(e.getValue());
      }
      this.btgIDs = Lists.newArrayList(newBtgIDs);
    }
  }

  /**
   * Initializes the internal map with default size when needed.
   */
  private void initNeighbourMinimBTGIDMap() {
    initNeighbourMinimumBTGIDMap(-1);
  }

  /**
   * Initializes the internal map with given size when needed. If size is -1 a
   * default map will be created.
   *
   * @param size the expected size of the Map or -1 if unknown.
   */
  private void initNeighbourMinimumBTGIDMap(int size) {
    if (size == -1) {
      this.neighborMinimumBTGIds = Maps.newHashMap();
    } else {
      this.neighborMinimumBTGIds = Maps.newHashMapWithExpectedSize(size);
    }
  }
}
