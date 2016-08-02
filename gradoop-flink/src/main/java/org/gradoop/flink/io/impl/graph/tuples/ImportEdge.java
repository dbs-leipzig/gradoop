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
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.io.impl.graph.tuples;

import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.common.util.GConstants;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents an external edge which can be imported into the EPGM.
 *
 * f0: edge identifier or null
 * f1: source vertex identifier
 * f2: target vertex identifier
 * f3: edge label
 * f4: edge properties
 *
 * @param <K> vertex/edge identifier type
 */
public class ImportEdge<K extends Comparable<K>>
  extends Tuple5<K, K, K, String, PropertyList> {

  /**
   * Default constructor for (de-)serialization.
   */
  public ImportEdge() { }

  /**
   * Creates a new import edge.
   *
   * @param edgeId    import edge id (i.e. identifier in source system)
   * @param sourceId  import source vertex id
   * @param targetId  import target vertex id
   */
  public ImportEdge(K edgeId, K sourceId, K targetId) {
    this(edgeId, sourceId, targetId, GConstants.DEFAULT_EDGE_LABEL);
  }

  /**
   * Creates a new import edge.
   *
   * @param edgeId    import edge id (i.e. identifier in source system)
   * @param sourceId  import source vertex id
   * @param targetId  import target vertex id
   * @param label     edge label
   */
  public ImportEdge(K edgeId, K sourceId, K targetId, String label) {
    this(edgeId, sourceId, targetId, label,
      PropertyList.createWithCapacity(0));
  }

  /**
   * Creates a new import edge.
   *
   * @param edgeId      import edge id (i.e. identifier in source system)
   * @param sourceId    import source vertex id
   * @param targetId    import target vertex id
   * @param label       edge label
   * @param properties  edge properties
   */
  public ImportEdge(K edgeId, K sourceId, K targetId, String label,
    PropertyList properties) {
    setId(edgeId);
    setSourceId(sourceId);
    setTargetId(targetId);
    setLabel(label);
    setProperties(properties);
  }

  public K getId() {
    return f0;
  }

  public void setId(K id) {
    this.f0 = checkNotNull(id, "id was null");
  }

  public K getSourceId() {
    return f1;
  }

  public void setSourceId(K sourceVertexId) {
    f1 = checkNotNull(sourceVertexId, "sourceId was null");
  }

  public K getTargetId() {
    return f2;
  }

  public void setTargetId(K targetVertexId) {
    f2 = checkNotNull(targetVertexId, "targetId was null");
  }

  public String getLabel() {
    return f3;
  }

  public void setLabel(String label) {
    f3 = checkNotNull(label, "label was null");
  }

  public PropertyList getProperties() {
    return f4;
  }

  public void setProperties(PropertyList properties) {
    f4 = checkNotNull(properties, "properties were null");
  }
}
