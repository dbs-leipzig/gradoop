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
import org.gradoop.common.model.impl.properties.PropertyList;

/**
 * This is the simplest form an embedding entry.
 * It consists only of the element id.
 */
public class Id implements EmbeddingEntry {
  /**
   * The elements identifier
   */
  private GradoopId id;

  /**
   * Create a new id entry
   * @param id the elements id
   */
  public Id(GradoopId id) {
    this.id = id;
  }

  /**
   * {@inheritDoc}
   */
  public GradoopId getId() { return id; }

  /**
   * Since we don't store any properties return an empty list
   * @return empty propertyList
   */
  public PropertyList getProperties() {
    return new PropertyList();
  }
}
