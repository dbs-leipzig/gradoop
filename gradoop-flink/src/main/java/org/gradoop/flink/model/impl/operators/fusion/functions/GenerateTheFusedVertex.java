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

package org.gradoop.flink.model.impl.operators.fusion.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 *
 * Creates the fused vertex from the collection of the graph head of the pattern graph element.
 * The new vertex is stored as an occurence of the searchGraph
 *
 * Created by Giacomo Bergami on 14/02/17.
 */
@FunctionAnnotation.ForwardedFieldsFirst("id")
@FunctionAnnotation.ForwardedFieldsSecond("label;properties")
public class GenerateTheFusedVertex implements FlatJoinFunction<GraphHead, GraphHead, Vertex> {

  public static final Vertex reusableVertex = new Vertex();
  public GradoopId newVertexId;

  public GenerateTheFusedVertex(GradoopId newVertexId) {
    this.newVertexId = newVertexId;
  }

  public GenerateTheFusedVertex() {
    this.newVertexId = null;
  }

  public GenerateTheFusedVertex updateVertexId(GradoopId id) {
    this.newVertexId = id;
    return this;
  }

  @Override
  public void join(GraphHead searchGraphHead, GraphHead patternGraphSeachHead,
    Collector<Vertex> out) throws Exception {
    reusableVertex.setLabel(patternGraphSeachHead.getLabel());
    reusableVertex.setProperties(patternGraphSeachHead.getProperties());
    reusableVertex.setId(newVertexId);
    reusableVertex.addGraphId(searchGraphHead.getId());
    out.collect(reusableVertex);
  }
}
