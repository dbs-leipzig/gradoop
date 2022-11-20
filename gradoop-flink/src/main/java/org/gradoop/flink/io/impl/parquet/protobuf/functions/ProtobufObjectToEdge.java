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
package org.gradoop.flink.io.impl.parquet.protobuf.functions;

import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.parquet.protobuf.EPGMProto;

/**
 * Creates an {@link EPGMEdge} from a protobuf {@link EPGMProto.Edge.Builder}.
 */
public class ProtobufObjectToEdge extends ProtobufObjectToElement<EPGMProto.Edge.Builder, EPGMEdge> {

  /**
   * Used to create the edge.
   */
  private final EdgeFactory<EPGMEdge> edgeFactory;

  /**
   * Creates ProtobufObjectToEdge converter.
   *
   * @param edgeFactory The factory that is used to create the edges.
   */
  public ProtobufObjectToEdge(EdgeFactory<EPGMEdge> edgeFactory) {
    this.edgeFactory = edgeFactory;
  }

  @Override
  public EPGMEdge map(EPGMProto.Edge.Builder value) throws Exception {
    GradoopId id = parseGradoopId(value.getId());
    String label = value.getLabel();
    GradoopId sourceVertexId = parseGradoopId(value.getSourceId());
    GradoopId targetVertexId = parseGradoopId(value.getTargetId());
    Properties properties = parseProperties(value.getPropertiesMap());
    GradoopIdSet graphIds = parseGradoopIds(value.getGraphIdsList());
    return edgeFactory.initEdge(id, label, sourceVertexId, targetVertexId, properties, graphIds);
  }
}
