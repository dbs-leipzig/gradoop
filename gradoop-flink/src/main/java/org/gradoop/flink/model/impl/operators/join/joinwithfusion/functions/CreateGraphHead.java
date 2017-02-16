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

package org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Created by vasistas on 16/02/17.
 */
public class CreateGraphHead implements MapFunction<Vertex, GraphHead> {

  private final GraphHead gh;

  public CreateGraphHead(GradoopId gid) {
    gh = new GraphHead();
    gh.setId(gid);
  }

  @Override
  public GraphHead map(Vertex value) throws Exception {
    gh.setProperties(value.getProperties());
    gh.setLabel(value.getLabel());
    return gh;
  }
}
