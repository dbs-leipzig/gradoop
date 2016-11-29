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

package org.gradoop.flink.algorithms.fsm.transactional.tle.fsm.pojos;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.transactional.tle.common.pojos.Embedding;
import org.gradoop.flink.algorithms.fsm.transactional.tle.common.pojos.FSMEdge;
import org.gradoop.flink.algorithms.fsm.transactional.tle.common.pojos.FSMGraph;

import java.util.Map;

/**
 * Lightweight representation of a labeled graph transaction.
 */
public class TFSMGraph extends Embedding implements FSMGraph {

  /**
   * graph identifier
   */
  private final GradoopId id;

  /**
   * Constructor.
   *
   * @param id graph identifier
   * @param vertices id-vertex map
   * @param edges id-edge map
   */
  public TFSMGraph(GradoopId id,
    Map<Integer, String> vertices, Map<Integer, FSMEdge> edges) {
    super(vertices, edges);
    this.id = id;
  }

  public GradoopId getId() {
    return id;
  }
}
