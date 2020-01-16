/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.storage.hbase.impl.io.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.storage.hbase.impl.api.VertexHandler;

/**
 * Creates HBase {@link Mutation} from vertex data using vertex data handler.
 */
public class BuildVertexMutation extends RichMapFunction<EPGMVertex, Tuple2<GradoopId, Mutation>> {

  /**
   * Serial version uid.
   */
  private static final long serialVersionUID = 42L;

  /**
   * Reusable tuple for each writer.
   */
  private transient Tuple2<GradoopId, Mutation> reuseTuple;

  /**
   * EPGMVertex data handler to create Mutations.
   */
  private final VertexHandler vertexHandler;

  /**
   * Creates rich map function.
   *
   * @param vertexHandler vertex data handler
   */
  public BuildVertexMutation(VertexHandler vertexHandler) {
    this.vertexHandler = vertexHandler;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    reuseTuple = new Tuple2<>();
  }

  @Override
  public Tuple2<GradoopId, Mutation> map(EPGMVertex vertex) throws Exception {
    GradoopId key = vertex.getId();
    Put put = new Put(vertexHandler.getRowKey(vertex.getId()));
    put = vertexHandler.writeVertex(put, vertex);

    reuseTuple.f0 = key;
    reuseTuple.f1 = put;
    return reuseTuple;
  }
}
