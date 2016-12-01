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

import com.google.common.collect.Lists;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;

import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.joining;

/**
 * Represents a path in an embedding.
 *
 * This is used for paths with variable length where access to specific elements is not necessary.
 */
public class PathEntry implements EmbeddingEntry {

  /**
   * Contains the path represented by element ids
   */
  private List<GradoopId> path;

  /**
   * Creates a new IdListEntry from a given path
   * @param path path representet bei element ids
   */
  public PathEntry(List<GradoopId> path) {
    this.path = path;
  }

  /**
   * Creates a new IdListEntry with empty path
   */
  public PathEntry() {
    this(Lists.newArrayList());
  }

  /**
   * Returns the first element of the path.
   *
   * @return first element of the path
   */
  @Override
  public GradoopId getId() {
    return path.get(0);
  }

  /**
   * Returns the path.
   *
   * @return path
   */
  public List<GradoopId> getPath() {
    return path;
  }

  /**
   * Adds a list of ids to the path.
   *
   * @param ids ids that will be added
   */
  public void addAll(List<GradoopId> ids) {
    path.addAll(ids);
  }

  /**
   * Adds a new Id to the path
   *
   * @param id id that will be added
   */
  public void add(GradoopId id) {
    path.add(id);
  }

  /**
   * Paths have no properties, this method returns an empty optional.
   *
   * @return empty optional
   */
  @Override
  public Optional<Properties> getProperties() {
    return Optional.empty();
  }

  @Override
  public boolean contains(GradoopId id) {
    return path.stream().anyMatch(pathId -> pathId.equals(id));
  }

  @Override
  public String toString() {
    return "(" + path.stream().map(GradoopId::toString).collect(joining(", ")) + ")";
  }
}

