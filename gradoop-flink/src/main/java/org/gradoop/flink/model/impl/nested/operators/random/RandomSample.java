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

package org.gradoop.flink.model.impl.nested.operators.random;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.nested.datastructures.DataLake;
import org.gradoop.flink.model.impl.nested.datastructures.IdGraphDatabase;
import org.gradoop.flink.model.impl.nested.operators.UnaryOp;
import org.gradoop.flink.model.impl.nested.operators.random.functions.RandomFilterOfGidTuple;
import org.gradoop.flink.model.impl.nested.operators.union.functions.SubsituteHead;

/**
 * Created by vasistas on 13/03/17.
 */
public class RandomSample extends UnaryOp {

  /**
   * relative amount of nodes in the result graph
   */
  private final float sampleSize;

  /**
   * seed for the random number generator
   * if no seed is null, the random generator is created without seed
   */
  private final long randomSeed;

  /**
   * Defines the new graph id
   */
  private final GradoopId gid;

  /**
   * Creates new RandomNodeSampling instance.
   *
   * @param sampleSize relative sample size
   */
  public RandomSample(float sampleSize) {
    this(GradoopId.get(),sampleSize, 0L);
  }

  /**
   * Creates new RandomNodeSampling instance.
   *
   * @param graphId    using a default graph id
   * @param sampleSize relative sample size
   * @param randomSeed random seed value (can be {@code null})
   */
  public RandomSample(GradoopId graphId, float sampleSize, long randomSeed) {
    this.sampleSize = sampleSize;
    this.randomSeed = randomSeed;
    this.gid = graphId;
  }

  @Override
  protected IdGraphDatabase runWithArgAndLake(DataLake lake, IdGraphDatabase data) {
    DataSet<Tuple2<GradoopId, GradoopId>> newVertices = data.getGraphHeadToVertex()
      .filter(new RandomFilterOfGidTuple(sampleSize, randomSeed))
      .flatMap(new SubsituteHead(gid));

    DataSet<GradoopId> head =
      lake.asNormalizedGraph().getConfig().getExecutionEnvironment().fromElements(gid);

    DataSet<GradoopId> edgeId = lake.getEdges()
      .join(newVertices)
      .where(new SourceId<>())
      .equalTo(new Value1Of2<>())
      .with(new LeftSide<>())
      .join(newVertices)
      .where(new TargetId<>())
      .equalTo(new Value1Of2<>())
      .with(new LeftSide<>())
      .map(new Id<>());

    DataSet<Tuple2<GradoopId, GradoopId>> newEdges = head
      .crossWithHuge(edgeId);

    return new IdGraphDatabase(head,newVertices,newEdges);
  }

  @Override
  public String getName() {
    return "RandomSample";
  }
}
