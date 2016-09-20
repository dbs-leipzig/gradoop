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

package org.gradoop.flink.algorithms.fsm.ccs.pojos;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.common.pojos.FSMEdge;
import org.gradoop.flink.algorithms.fsm.tfsm.pojos.TFSMGraph;

import java.util.Map;

/**
 * Lightweight representation of a labeled graph transaction.
 */
public class CCSGraph extends TFSMGraph {

  /**
   * graph identifier
   */
  private final String category;

  /**
   * Constructor.
   *
   * @param category category
   * @param id graph identifier
   * @param vertices id-vertex map
   * @param edges id-edge map
   */
  public CCSGraph(String category, GradoopId id,
    Map<Integer, String> vertices, Map<Integer, FSMEdge> edges) {
    super(id, vertices, edges);
    this.category = category;
  }

  public String getCategory() {
    return category;
  }
}
