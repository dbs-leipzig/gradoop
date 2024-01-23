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
package org.gradoop.temporal.io.impl.parquet.plain.read;

import org.apache.parquet.schema.MessageType;
import org.gradoop.flink.io.impl.parquet.plain.common.GradoopRootConverter;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;

/**
 * Root parquet group converter for temporal graph heads.
 */
public class TemporalGraphHeadRootConverter extends GradoopRootConverter<TemporalGraphHead> {

  /**
   * Creates a new root converter for temporal graph heads.
   *
   * @param requestedSchema the record type
   */
  public TemporalGraphHeadRootConverter(MessageType requestedSchema) {
    super(requestedSchema);
  }

  @Override
  protected void initializeConverters() {
    this.registerConverter("id",
      new GradoopIdConverter(value -> this.record.setId(value)));
    this.registerConverter("label",
      new StringConverter(value -> this.record.setLabel(value)));
    this.registerConverter("properties",
      new PropertiesConverter(value -> this.record.setProperties(value)));
    this.registerConverter("transaction_time",
      new TimeIntervalConverter(value -> this.record.setTransactionTime(value)));
    this.registerConverter("valid_time",
      new TimeIntervalConverter(value -> this.record.setValidTime(value)));
  }

  @Override
  protected TemporalGraphHead createRecord() {
    return new TemporalGraphHead();
  }
}
