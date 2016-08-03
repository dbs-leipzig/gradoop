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

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.common.util.GConstants;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents an external vertex  which can be imported into the EPGM.
 *
 * f0: external vertex identifier
 * f1: vertex label
 * f2: vertex properties
 *
 * @param <K> vertex/edge identifier type
 */
public class ImportVertex<K extends Comparable<K>>
  extends Tuple3<K, String, PropertyList> {

  /**
   * Default constructor for (de-)serialization.
   */
  public ImportVertex() { }

  /**
   * Creates a new import vertex.
   *
   * @param id import vertex id (i.e. identifier in the source system)
   */
  public ImportVertex(K id) {
    this(id, GConstants.DEFAULT_VERTEX_LABEL);
  }

  /**
   * Creates a new import vertex.
   *
   * @param id    import vertex id (i.e. identifier in the source system)*
   * @param label vertex label
   */
  public ImportVertex(K id, String label) {
    this(id, label, PropertyList.createWithCapacity(0));
  }


  /**
   * Creates a new import vertex.
   *
   * @param id          import vertex id (i.e. identifier in the source system)
   * @param label       vertex label
   * @param properties  vertex properties
   */
  public ImportVertex(K id, String label, PropertyList properties) {
    setId(id);
    setLabel(label);
    setProperties(properties);
  }

  public K getId() {
    return f0;
  }

  public void setId(K id) {
    f0 = checkNotNull(id, "id was null");
  }

  public String getLabel() {
    return f1;
  }

  public void setLabel(String label) {
    f1 = checkNotNull(label, "label was null");
  }

  public PropertyList getProperties() {
    return f2;
  }

  public void setProperties(PropertyList properties) {
    f2 = checkNotNull(properties, "properties were null");
  }
}
