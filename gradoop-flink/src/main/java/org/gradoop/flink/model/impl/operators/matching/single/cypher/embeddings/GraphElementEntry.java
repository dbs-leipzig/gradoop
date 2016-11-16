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
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.properties.Properties;

import java.util.Optional;

/**
 * This embedding entry wraps a graph element like vertex or edge
 */
public class GraphElementEntry implements EmbeddingEntry {
  /**
   * Holds the wrapped graph element
   */
  private final GraphElement graphElement;

  /**
   * Create a new graph element embedding entry
   * @param graphElement the wrapped graph element
   */
  public GraphElementEntry(GraphElement graphElement) {
    this.graphElement = graphElement;
  }

  /**
   * {@inheritDoc}
   */
  public GradoopId getId() {
    return graphElement.getId();
  }

  /**
   * Return the GraphElements property list
   * @return property list
   */
  public Optional<Properties> getProperties() {
    return Optional.of(graphElement.getProperties());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    GraphElementEntry that = (GraphElementEntry) o;

    return graphElement != null ? graphElement.equals(that.graphElement) :
      that.graphElement == null;

  }

  @Override
  public int hashCode() {
    return graphElement != null ? graphElement.hashCode() : 0;
  }
}
