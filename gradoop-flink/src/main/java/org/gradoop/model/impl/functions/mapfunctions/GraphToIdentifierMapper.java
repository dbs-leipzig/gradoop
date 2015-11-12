/*
 * This file is part of gradoop.
 *
 * gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.functions.mapfunctions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Maps a graph to its identifier.
 *
 * @param <GD> EPGM graph head type
 */
public class GraphToIdentifierMapper<GD extends EPGMGraphHead>
  implements MapFunction<GD, GradoopId> {

  @Override
  public GradoopId map(GD graphHead) throws Exception {
    return graphHead.getId();
  }
}
