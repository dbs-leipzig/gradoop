/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.storage.impl.hbase.io.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.storage.impl.hbase.api.GraphHeadHandler;

/**
 * Creates HBase {@link Mutation} from persistent graph data using graph
 * data handler.
 */
public class BuildGraphHeadMutation extends
  RichMapFunction<GraphHead, Tuple2<GradoopId, Mutation>> {

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
  public Tuple2<GradoopId, Mutation> map(GraphHead graphHead) throws Exception {
    GradoopId key = graphHead.getId();
    Put put = new Put(graphHeadHandler.getRowKey(graphHead.getId()));
    put = graphHeadHandler.writeGraphHead(put, graphHead);

    reuseTuple.f0 = key;
    reuseTuple.f1 = put;
    return reuseTuple;
  }
}
