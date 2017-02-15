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

package org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.join.JoinType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.utils.JoinWithJoinsUtils;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.utils.OptSerializableGradoopId;

/**
 * Given a set of vertex elements that have to be pre-filtered accordingly to the vertex join
 * semantics, performs a pre-filtering and pre-processing step by selecting only those vertices
 * that really will be involved in the final result.
 *
 * Created by Giacomo Bergami on 14/02/17.
 */
public class PreFilterRelationalJoin implements PreFilter<Vertex> {

  /**
   * Checking if the vertex dataset comes from the left operand or not
   */
  private final boolean isLeft;

  /**
   * Edge expressing the relations
   */
  private final DataSet<Edge> e;

  /**
   * How the vertices have to be joined
   */
  private final JoinType vertexJoinType;

  /**
   * Default constructor
   * @param isLeft          Set to false if this UDF is used for the left operand,
   *                        true otherwise
   * @param e               Edge set expressing the relations
   * @param vertexJoinType  How the vertices have to be joined
   */
  public PreFilterRelationalJoin(Boolean isLeft, DataSet<Edge> e, JoinType vertexJoinType) {
    this.isLeft = isLeft;
    this.e = e;
    this.vertexJoinType = vertexJoinType;
  }

  @Override
  public DataSet<Tuple2<Vertex, OptSerializableGradoopId>> apply(DataSet<Vertex> vertexDataSet) {
    return  JoinWithJoinsUtils.joinByVertexEdge(vertexDataSet, e, vertexJoinType, isLeft)
      .where(new Id<>())
      .equalTo(new KeySelectorFromVertexSrcOrDest(isLeft))
      .with(new JoinFunctionWithVertexAndGradoopIdFromEdge());
  }
}
