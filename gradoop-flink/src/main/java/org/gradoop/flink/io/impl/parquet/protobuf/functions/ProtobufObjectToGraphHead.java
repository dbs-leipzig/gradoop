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

import org.gradoop.common.model.api.entities.GraphHeadFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.parquet.protobuf.EPGMProto;

/**
 * Creates an {@link EPGMGraphHead} from a protobuf {@link EPGMProto.GraphHead.Builder}.
 */
public class ProtobufObjectToGraphHead extends ProtobufObjectToElement<EPGMProto.GraphHead.Builder, EPGMGraphHead> {

  /**
   * Used to create the graph head.
   */
  private final GraphHeadFactory<EPGMGraphHead> graphHeadFactory;

  /**
   * Creates ProtobufObjectToGraphHead converter.
   *
   * @param graphHeadFactory The factory that is used to create the graph heads.
   */
  public ProtobufObjectToGraphHead(GraphHeadFactory<EPGMGraphHead> graphHeadFactory) {
    this.graphHeadFactory = graphHeadFactory;
  }

  @Override
  public EPGMGraphHead map(EPGMProto.GraphHead.Builder value) throws Exception {
    GradoopId id = parseGradoopId(value.getId());
    String label = value.getLabel();
    Properties properties = parseProperties(value.getPropertiesMap());
    return this.graphHeadFactory.initGraphHead(id, label, properties);
  }
}
