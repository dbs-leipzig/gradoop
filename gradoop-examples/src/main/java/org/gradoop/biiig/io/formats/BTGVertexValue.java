/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.biiig.io.formats;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.io.Writable;
import org.gradoop.model.GraphElement;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Custom vertex used by {@link org.gradoop.biiig.algorithms.BTGComputation}.
 */
public class BTGVertexValue implements Writable, GraphElement {

  /**
   * The vertex type which is defined in {@link BTGVertexType}
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
   * vertices of type {@link BTGVertexType} MASTER, so it is only initialized
   * when needed.
   */
  private Map<Long, Long> neighborMinimumBTGIds;

  /**
   * Default constructor which is used during deserialization in {@link
   * BTGTextVertexOutputFormat}
   */
  @SuppressWarnings("UnusedDeclaration")
  public BTGVertexValue() {
    this.btgIDs = new ArrayList<>();
  }

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
  @Override
  public Iterable<Long> getGraphs() {
    return this.btgIDs;
  }

  /**
   * Adds a BTG to the list of BTGs. BTGs can occur multiple times.
   *
   * @param graph BTG ID to be added
   */
  @Override
  public void addGraph(Long graph) {
    if (this.btgIDs.isEmpty()) {
      resetGraphs();
    }
    this.btgIDs.add(graph);
  }

  @Override
  public void resetGraphs() {
    this.btgIDs = Lists.newArrayList();
  }

  @Override
  public void addGraphs(Iterable<Long> graphs) {
    if (this.btgIDs.isEmpty()) {
      resetGraphs();
    }
    for (Long btg : graphs) {
      this.btgIDs.add(btg);
    }
  }

  @Override
  public int getGraphCount() {
    return this.btgIDs.size();
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
   * ids. This is only necessary for master data nodes like described in {@link
   * org.gradoop.biiig.algorithms.BTGComputation}.
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

  /**
   * Serializes the content of the vertex object.
   *
   * @param dataOutput data to be serialized
   * @throws java.io.IOException
   */
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    // vertex type
    dataOutput.writeInt(this.vertexType.ordinal());
    // vertex value
    dataOutput.writeDouble(this.vertexValue);
    // number of BTGs
    dataOutput.writeInt(this.btgIDs.size());
    // BTG ids
    for (Long btgID : this.btgIDs) {
      dataOutput.writeLong(btgID);
    }
    // neighbor -> minimum BTG id map
    if (this.neighborMinimumBTGIds != null) {
      dataOutput.writeInt(this.neighborMinimumBTGIds.size());
      for (Map.Entry<Long, Long> e : this.neighborMinimumBTGIds.entrySet()) {
        dataOutput.writeLong(e.getKey());
        dataOutput.writeLong(e.getValue());
      }
    } else {
      // need to write 0 for correct reading
      dataOutput.writeInt(0);
    }
  }

  /**
   * Deserializes the content of the vertex object.
   *
   * @param dataInput data to be deserialized
   * @throws java.io.IOException
   */
  @Override
  public void readFields(DataInput dataInput) throws IOException {
    // vertex type
    this.vertexType = BTGVertexType.values()[dataInput.readInt()];
    // vertex value
    this.vertexValue = dataInput.readDouble();
    // number of BTGs
    int btgCount = dataInput.readInt();
    // BTG ids
    for (int i = 0; i < btgCount; i++) {
      this.addGraph(dataInput.readLong());
    }
    // neighbor -> minium BTG id map
    int setSize = dataInput.readInt();
    if (setSize > 0) {
      initNeighbourMinimumBTGIDMap(setSize);
    }
    for (int i = 0; i < setSize; i++) {
      this.neighborMinimumBTGIds
        .put(dataInput.readLong(), dataInput.readLong());
    }
  }
}
