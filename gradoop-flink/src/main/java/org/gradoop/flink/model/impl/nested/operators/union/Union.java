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

package org.gradoop.flink.model.impl.nested.operators.union;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.nested.datastructures.IdGraphDatabase;
import org.gradoop.flink.model.impl.nested.operators.UnaryOp;
import org.gradoop.flink.model.impl.nested.datastructures.DataLake;
import org.gradoop.flink.model.impl.nested.operators.union.functions.SubsituteHead;

/**
 * Implements the union for the nesting model
 */
public class Union extends UnaryOp {

  /**
   * Graph id for the final resulting graph
   */
  private final GradoopId id;

  /**
   * Use a specific graph id for the graph that has to be returned
   * @param id  The aforementioned id
   */
  public Union(GradoopId id) {
    this.id = id;
  }

  /**
   * Use a random graph id for the final union graph
   */
  public Union() {
    this(GradoopId.get());
  }

  @Override
  protected IdGraphDatabase runWithArgAndLake(DataLake lake, IdGraphDatabase data) {
    DataSet<GradoopId> head = lake.asNormalizedGraph().getConfig().getExecutionEnvironment()
      .fromElements(id);
    DataSet<Tuple2<GradoopId, GradoopId>> vertices = data.getGraphHeadToVertex()
      .flatMap(new SubsituteHead(id))
      .distinct(1);
    DataSet<Tuple2<GradoopId, GradoopId>> edges = data.getGraphHeadToEdge()
      .flatMap(new SubsituteHead(id))
      .distinct(1);
    return new IdGraphDatabase(head, vertices, edges);
  }

  @Override
  public String getName() {
    return Union.class.getName();
  }

}
