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

package org.gradoop.model.impl;

import com.google.common.collect.Sets;
import org.gradoop.model.EPGraphElement;

import java.util.Map;
import java.util.Set;

public abstract class EPFlinkGraphElementEntity extends EPFlinkEntity implements
  EPGraphElement {

  private Set<Long> graphs;

  public EPFlinkGraphElementEntity() {
    this.graphs = Sets.newHashSet();
  }

  public EPFlinkGraphElementEntity(Set<Long> graphs) {
    this.graphs = graphs;
  }

  public EPFlinkGraphElementEntity(EPFlinkGraphElementEntity otherEntity) {
    super(otherEntity);
    this.graphs = Sets.newHashSet(otherEntity.graphs);
  }

  public EPFlinkGraphElementEntity(Long id, String label,
    Map<String, Object> properties, Set<Long> graphs) {
    super(id, label, properties);
    if (graphs != null) {
      this.graphs = graphs;
    } else {
      this.graphs = Sets.newHashSet();
    }
  }

  @Override
  public Set<Long> getGraphs() {
    return this.graphs;
  }

  @Override
  public void setGraphs(Set<Long> graphs) {
    this.graphs = graphs;
  }

  @Override
  public void addGraph(Long graph) {
    this.graphs.add(graph);
  }

  @Override
  public String toString() {
    return "EPFlinkGraphElementEntity{" +
      "super=" + super.toString() +
      ", graphs=" + graphs +
      '}';
  }
}
