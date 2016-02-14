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

package org.gradoop.io.graph.tuples;

import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.model.impl.properties.PropertyList;

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
   * @param edgeId          import edge id (i.e. identifier in source system)
   * @param sourceVertexId  import source vertex id
   * @param targetVertexId  import target vertex id
   * @param label           edge label
   * @param properties      edge properties
   */
  public ImportEdge(K edgeId,
    K sourceVertexId,
    K targetVertexId,
    String label,
    PropertyList properties) {

    setId(edgeId);
    setSourceVertexId(sourceVertexId);
    setTargetVertexId(targetVertexId);
    setLabel(label);
    setProperties(properties);
  }

  public K getId() {
    return f0;
  }

  public void setId(K id) {
    this.f0 = checkNotNull(id, "id was null");
  }

  public K getSourceVertexId() {
    return f1;
  }

  public void setSourceVertexId(K sourceVertexId) {
    f1 = checkNotNull(sourceVertexId, "sourceVertexId was null");
  }

  public K getTargetVertexId() {
    return f2;
  }

  public void setTargetVertexId(K targetVertexId) {
    f2 = checkNotNull(targetVertexId, "targetVertexId was null");
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
