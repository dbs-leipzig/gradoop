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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;

import java.util.Optional;

/**
 * Represents an entry in an embedding
 */
public interface EmbeddingEntry {
  /**
   * Returns the identifier of the element
   * @return id
   */
  GradoopId getId();

  /**
   * Returns the list of properties of this element
   * @return properties
   */
  Optional<Properties>  getProperties();

  /**
   * Checks if the embedding entry contains the specified id
   * @param id the query id
   * @return true if the id is contained in the embedding entry
   */
  Boolean contains(GradoopId id);
}
