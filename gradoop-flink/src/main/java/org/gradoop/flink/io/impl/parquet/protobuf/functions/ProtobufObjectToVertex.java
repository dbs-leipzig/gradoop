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

import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.parquet.protobuf.EPGMProto;

/**
 * Creates an {@link EPGMVertex} from a protobuf {@link EPGMProto.Vertex.Builder}.
 */
public class ProtobufObjectToVertex extends ProtobufObjectToElement<EPGMProto.Vertex.Builder, EPGMVertex> {

  /**
   * Used to create the vertex.
   */
  private final VertexFactory<EPGMVertex> vertexFactory;

  /**
   * Creates ProtobufObjectToVertex converter.
   *
   * @param vertexFactory The factory that is used to create the vertices.
   */
  public ProtobufObjectToVertex(VertexFactory<EPGMVertex> vertexFactory) {
    this.vertexFactory = vertexFactory;
  }

  @Override
  public EPGMVertex map(EPGMProto.Vertex.Builder value) throws Exception {
    GradoopId id = parseGradoopId(value.getId());
    String label = value.getLabel();
    Properties properties = parseProperties(value.getPropertiesMap());
    GradoopIdSet graphIds = parseGradoopIds(value.getGraphIdsList());
    return vertexFactory.initVertex(id, label, properties, graphIds);
  }
}
