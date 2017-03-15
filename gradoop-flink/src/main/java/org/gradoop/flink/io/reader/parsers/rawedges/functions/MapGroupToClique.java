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

package org.gradoop.flink.io.reader.parsers.rawedges.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.List;

/**
 * Maps each group to a different clique. Defines the vertices for the IdGraphDatabase
 */
public class MapGroupToClique<Element> implements
  FlatJoinFunction<Tuple3<Element, GradoopId, Vertex>, Tuple2<GradoopId, List<Element>>,
    Tuple2<GradoopId, GradoopId>> {


  @Override
  public void join(Tuple3<Element, GradoopId, Vertex> first,
    Tuple2<GradoopId, List<Element>> second, Collector<Tuple2<GradoopId, GradoopId>> out) throws
    Exception {

  }
}
