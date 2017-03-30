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
package org.gradoop.flink.model.impl.operators.nest2.model.indices;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.nest.tuples.Hexaplet;
import org.gradoop.flink.model.impl.operators.nest2.model.VertexCentricResult;

/**
 * Representing a NestingIndexing preserving part of the previous computation that could be
 * reused in further operations. This information is preserved as Hexaplets, that is the
 * matching between the vertices.
 */
public class IndexingForNesting extends NestedIndexing implements
  VertexCentricResult<DataSet<Hexaplet>> {

  /**
   * Representation of the intermediate state of the previous computation
   */
  private final DataSet<Hexaplet> state;

  /**
   * Creating an instance of the graph database by just using the elements' ids
   *
   * @param graphHeads        The heads defining the component at the first level of annidation
   * @param graphHeadToVertex The vertices defining the components at the intermediately down
   *                          level
   * @param graphHeadToEdge   The edges appearing between each possible level
   */
  public IndexingForNesting(DataSet<GradoopId> graphHeads,
    DataSet<Tuple2<GradoopId, GradoopId>> graphHeadToVertex,
    DataSet<Tuple2<GradoopId, GradoopId>> graphHeadToEdge,
    DataSet<Hexaplet> state) {
    super(graphHeads, graphHeadToVertex, graphHeadToEdge);
    this.state = state;
  }

  @Override
  public DataSet<Hexaplet> getPreviousComputation() {
    return state;
  }

}
