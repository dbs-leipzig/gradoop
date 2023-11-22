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
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.parquet.protobuf.functions.ProtobufObjectToElement;
import org.gradoop.temporal.io.impl.parquet.protobuf.TPGMProto;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

/**
 * Creates an {@link TemporalEdge} from a protobuf {@link TPGMProto.TemporalEdge.Builder}
 */
public class ProtobufObjectToTemporalEdge extends
  ProtobufObjectToElement<TPGMProto.TemporalEdge.Builder, TemporalEdge> {

  /**
   * Used to create the edge.
   */
  private final EdgeFactory<TemporalEdge> edgeFactory;

  /**
   * Creates ProtobufObjectToTemporalEdge converter
   *
   * @param edgeFactory The factory that is used to create the edges.
   */
  public ProtobufObjectToTemporalEdge(EdgeFactory<TemporalEdge> edgeFactory) {
    this.edgeFactory = edgeFactory;
  }

  @Override
  public TemporalEdge map(TPGMProto.TemporalEdge.Builder value) throws Exception {
    GradoopId id = parseGradoopId(value.getId());
    String label = value.getLabel();
    GradoopId sourceVertexId = parseGradoopId(value.getSourceId());
    GradoopId targetVertexId = parseGradoopId(value.getTargetId());
    Properties properties = parseProperties(value.getPropertiesMap());
    GradoopIdSet graphIds = parseGradoopIds(value.getGraphIdsList());

    TemporalEdge edge = edgeFactory.initEdge(id, label, sourceVertexId, targetVertexId, properties, graphIds);
    edge.setTransactionTime(new Tuple2<>(value.getTxFrom(), value.getTxTo()));
    edge.setValidTime(new Tuple2<>(value.getValFrom(), value.getValTo()));

    return edge;
  }
}
