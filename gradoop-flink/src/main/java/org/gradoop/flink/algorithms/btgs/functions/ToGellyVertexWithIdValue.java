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

package org.gradoop.flink.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * EPGM vertex to gelly vertex, where value is vertex id
 */
@FunctionAnnotation.ForwardedFields("id->f0;id->f1")
public class ToGellyVertexWithIdValue implements
  MapFunction<Vertex, org.apache.flink.graph.Vertex<GradoopId, GradoopId>> {

  /**
   * Reduce object instantiations
   */
  private final org.apache.flink.graph.Vertex<GradoopId, GradoopId> reuse =
    new org.apache.flink.graph.Vertex<>();

  @Override
  public org.apache.flink.graph.Vertex<GradoopId, GradoopId> map(Vertex vertex)
      throws Exception {
    GradoopId id = vertex.getId();
    reuse.setId(id);
    reuse.setValue(id);
    return reuse;
  }
}
