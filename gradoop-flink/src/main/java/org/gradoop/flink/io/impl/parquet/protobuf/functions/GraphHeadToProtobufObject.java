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

import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.flink.io.impl.parquet.protobuf.EPGMProto;

/**
 * Creates a protobuf {@link EPGMProto.GraphHead.Builder} from an {@link EPGMGraphHead}.
 */
public class GraphHeadToProtobufObject extends ElementToProtobufObject<EPGMGraphHead, EPGMProto.GraphHead.Builder> {

  @Override
  protected EPGMProto.GraphHead.Builder newBuilder() {
    return EPGMProto.GraphHead.newBuilder();
  }

  @Override
  public EPGMProto.GraphHead.Builder map(EPGMGraphHead value) throws Exception {
    return this.resetAndGetBuilder()
      .setId(serializeGradoopId(value.getId()))
      .setLabel(value.getLabel())
      .putAllProperties(serializeProperties(value));
  }
}
