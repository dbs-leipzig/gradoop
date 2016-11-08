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

import com.google.common.collect.Lists;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyList;

import java.util.List;
import java.util.Optional;

/**
 * Represents a path in an embedding.
 * This is used e.g. for paths with variable path length where the access specific
 * elements is not necessary.
 */
public class ListEntry implements EmbeddingEntry{
  private List<GradoopId> path;

  /**
   * Creates a new ListEntry from a given path
   * @param path path representet bei element ids
   */
  public ListEntry(List<GradoopId> path) {
    this.path = path;
  }

  /**
   * Creates a new ListEntry with empty path
   */
  public ListEntry() {
    this(Lists.newArrayList());
  }

  @Override
  public GradoopId getId() {
    return path.get(0);
  }

  /**
   * ListEntries do not have properties so return nothing
   * @return empty optional
   */
  @Override
  public Optional<PropertyList> getProperties() {
    return Optional.empty();
  }
}

