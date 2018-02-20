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

package org.gradoop.flink.io.impl.xmlbasedcsv.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.xmlbasedcsv.XMLBasedCSVConstants;

/**
 * Creates a tuple containing the string representation of a vertex key and its id.
 */
@FunctionAnnotation.ForwardedFields("id->f1")
public class VertexToVertexIds implements MapFunction<Vertex, Tuple2<String, GradoopId>> {

  /**
   * Avoid object instantiation.
   */
  private Tuple2<String, GradoopId> reuseTuple = new Tuple2<>();

  @Override
  public Tuple2<String, GradoopId> map(Vertex vertex) throws Exception {
    reuseTuple.f0 = vertex.getPropertyValue(XMLBasedCSVConstants.PROPERTY_KEY_KEY).getString();
    reuseTuple.f1 = vertex.getId();
    return reuseTuple;
  }
}
