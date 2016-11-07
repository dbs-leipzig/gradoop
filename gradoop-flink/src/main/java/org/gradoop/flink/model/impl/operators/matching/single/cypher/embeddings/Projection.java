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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyList;

public class Projection implements EmbeddingEntry {
  private GradoopId id;
  private PropertyList properties;

  public Projection(GradoopId id) {
    this.properties = new PropertyList();
    this.id = id;
  }

  public GradoopId getId() {
    return id;
  }

  public PropertyList getProperties() { return properties; }

  public void addProperty(Property property) {
    properties.set(property);
  }

  public void setProperties (PropertyList properties) {
    this.properties = properties;
  }
}
