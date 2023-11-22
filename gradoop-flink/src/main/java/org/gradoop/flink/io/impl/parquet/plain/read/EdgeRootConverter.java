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
package org.gradoop.flink.io.impl.parquet.plain.read;

import org.apache.parquet.schema.MessageType;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.flink.io.impl.parquet.plain.common.GradoopRootConverter;

/**
 * Root parquet group converter for EPGM edges.
 */
public class EdgeRootConverter extends GradoopRootConverter<EPGMEdge> {

  /**
   * Creates a new root converter for EPGM edges.
   *
   * @param requestedSchema the record type
   */
  public EdgeRootConverter(MessageType requestedSchema) {
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
    this.registerConverter("graph_ids",
      new GradoopIdSetConverter(value -> this.record.setGraphIds(value)));
    this.registerConverter("source_id",
      new GradoopIdConverter(value -> this.record.setSourceId(value)));
    this.registerConverter("target_id",
      new GradoopIdConverter(value -> this.record.setTargetId(value)));
  }

  @Override
  protected EPGMEdge createRecord() {
    return new EPGMEdge();
  }
}
