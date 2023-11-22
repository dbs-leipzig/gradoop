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

import org.gradoop.flink.io.impl.parquet.protobuf.functions.ElementToProtobufObject;
import org.gradoop.temporal.io.impl.parquet.protobuf.TPGMProto;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

/**
 * Creates a protobuf {@link TPGMProto.TemporalVertex.Builder} from a {@link TemporalVertex}
 */
public class TemporalVertexToProtobufObject extends
  ElementToProtobufObject<TemporalVertex, TPGMProto.TemporalVertex.Builder> {

  @Override
  protected TPGMProto.TemporalVertex.Builder newBuilder() {
    return TPGMProto.TemporalVertex.newBuilder();
  }

  @Override
  public TPGMProto.TemporalVertex.Builder map(TemporalVertex value) throws Exception {
    return this.resetAndGetBuilder()
      .setId(serializeGradoopId(value.getId()))
      .setLabel(value.getLabel())
      .putAllProperties(serializeProperties(value))
      .addAllGraphIds(serializeGradoopIds(value.getGraphIds()))
      .setTxFrom(value.getTxFrom())
      .setTxTo(value.getTxTo())
      .setValFrom(value.getValidFrom())
      .setValTo(value.getValidTo());
  }
}
