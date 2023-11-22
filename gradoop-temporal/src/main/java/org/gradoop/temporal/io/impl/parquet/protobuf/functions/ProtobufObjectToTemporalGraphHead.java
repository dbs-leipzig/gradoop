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
import org.gradoop.common.model.api.entities.GraphHeadFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.parquet.protobuf.functions.ProtobufObjectToElement;
import org.gradoop.temporal.io.impl.parquet.protobuf.TPGMProto;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;

/**
 * Creates an {@link TemporalGraphHead} from a protobuf {@link TPGMProto.TemporalGraphHead.Builder}
 */
public class ProtobufObjectToTemporalGraphHead extends
  ProtobufObjectToElement<TPGMProto.TemporalGraphHead.Builder, TemporalGraphHead> {

  /**
   * Used to create the graph head.
   */
  private final GraphHeadFactory<TemporalGraphHead> graphHeadFactory;

  /**
   * Creates ProtobufObjectToTemporalGraphHead converter
   *
   * @param graphHeadFactory The factory that is used to create the graph heads.
   */
  public ProtobufObjectToTemporalGraphHead(GraphHeadFactory<TemporalGraphHead> graphHeadFactory) {
    this.graphHeadFactory = graphHeadFactory;
  }

  @Override
  public TemporalGraphHead map(TPGMProto.TemporalGraphHead.Builder value) throws Exception {
    GradoopId id = parseGradoopId(value.getId());
    String label = value.getLabel();
    Properties properties = parseProperties(value.getPropertiesMap());

    TemporalGraphHead graphHead = this.graphHeadFactory.initGraphHead(id, label, properties);
    graphHead.setTransactionTime(new Tuple2<>(value.getTxFrom(), value.getTxTo()));
    graphHead.setValidTime(new Tuple2<>(value.getValFrom(), value.getValTo()));

    return graphHead;
  }
}
