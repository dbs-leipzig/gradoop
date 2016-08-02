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

package org.gradoop.flink.io.impl.hbase.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.storage.api.GraphHeadHandler;
import org.gradoop.common.storage.api.PersistentGraphHead;

/**
 * Creates HBase {@link Mutation} from persistent graph data using graph
 * data handler.
 */
public class BuildGraphHeadMutation extends RichMapFunction
  <PersistentGraphHead, Tuple2<GradoopId, Mutation>> {

  /**
   * Serial version uid.
   */
  private static final long serialVersionUID = 42L;

  /**
   * Reusable tuple for each writer.
   */
  private transient Tuple2<GradoopId, Mutation> reuseTuple;

  /**
   * Graph data handler to create Mutations.
   */
  private final GraphHeadHandler graphHeadHandler;

  /**
   * Creates rich map function.
   *
   * @param graphHeadHandler graph data handler
   */
  public BuildGraphHeadMutation(GraphHeadHandler graphHeadHandler) {
    this.graphHeadHandler = graphHeadHandler;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    reuseTuple = new Tuple2<>();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple2<GradoopId, Mutation> map(
    PersistentGraphHead persistentGraphData) throws Exception {
    GradoopId key = persistentGraphData.getId();
    Put put = new Put(graphHeadHandler.getRowKey(persistentGraphData.getId()));
    put = graphHeadHandler.writeGraphHead(put, persistentGraphData);

    reuseTuple.f0 = key;
    reuseTuple.f1 = put;
    return reuseTuple;
  }
}
