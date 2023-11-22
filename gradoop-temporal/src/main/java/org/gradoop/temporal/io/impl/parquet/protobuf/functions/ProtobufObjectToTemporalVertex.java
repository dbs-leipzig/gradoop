/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.io.impl.parquet.protobuf.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.parquet.protobuf.functions.ProtobufObjectToElement;
import org.gradoop.temporal.io.impl.parquet.protobuf.TPGMProto;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

/**
 * Creates an {@link TemporalVertex} from a protobuf {@link TPGMProto.TemporalVertex.Builder}
 */
public class ProtobufObjectToTemporalVertex extends
  ProtobufObjectToElement<TPGMProto.TemporalVertex.Builder, TemporalVertex> {

  /**
   * Used to create the vertex.
   */
  private final VertexFactory<TemporalVertex> vertexFactory;

  /**
   * Creates ProtobufObjectToTemporalVertex converter
   *
   * @param vertexFactory The factory that is used to create the vertices.
   */
  public ProtobufObjectToTemporalVertex(VertexFactory<TemporalVertex> vertexFactory) {
    this.vertexFactory = vertexFactory;
  }

  @Override
  public TemporalVertex map(TPGMProto.TemporalVertex.Builder value) throws Exception {
    GradoopId id = parseGradoopId(value.getId());
    String label = value.getLabel();
    Properties properties = parseProperties(value.getPropertiesMap());
    GradoopIdSet graphIds = parseGradoopIds(value.getGraphIdsList());

    TemporalVertex vertex = vertexFactory.initVertex(id, label, properties, graphIds);
    vertex.setTransactionTime(new Tuple2<>(value.getTxFrom(), value.getTxTo()));
    vertex.setValidTime(new Tuple2<>(value.getValFrom(), value.getValTo()));

    return vertex;
  }
}
