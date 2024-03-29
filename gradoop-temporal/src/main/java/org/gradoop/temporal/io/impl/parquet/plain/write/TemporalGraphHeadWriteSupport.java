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
package org.gradoop.temporal.io.impl.parquet.plain.write;

import org.apache.parquet.schema.MessageType;
import org.gradoop.flink.io.impl.parquet.plain.common.GradoopParquetTypeBuilder;
import org.gradoop.flink.io.impl.parquet.plain.common.GradoopWriteSupport;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;

/**
 * Parquet write support for TPGM graph heads.
 */
public class TemporalGraphHeadWriteSupport extends GradoopWriteSupport<TemporalGraphHead> {

  @Override
  public MessageType getMessageType() {
    return new GradoopParquetTypeBuilder()
      .addGradoopIdField("id")
      .addStringField("label")
      .addPropertiesField("properties")
      .addTimeInterval("transaction_time")
      .addTimeInterval("valid_time")
      .build("temporal_graph_head");
  }

  @Override
  public void writeRecord(TemporalGraphHead record) {
    writeGradoopId("id", 0, record.getId());
    writeString("label", 1, record.getLabel());
    writeProperties("properties", 2, record.getProperties());
    writeTimeIntervalField("transaction_time", 3, record.getTransactionTime());
    writeTimeIntervalField("valid_time", 4, record.getValidTime());
  }
}
