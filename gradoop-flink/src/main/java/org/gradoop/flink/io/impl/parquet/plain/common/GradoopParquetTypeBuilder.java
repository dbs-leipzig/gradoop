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
package org.gradoop.flink.io.impl.parquet.plain.common;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Simple builder for parquet types with support for common EPGM/TPGM field types.
 */
public class GradoopParquetTypeBuilder {

  /**
   * the internal parquet type builder
   */
  private final Types.GroupBuilder<MessageType> messageTypeBuilder = Types.buildMessage();

  /**
   * Adds a parquet type for a gradoop id field.
   *
   * @param label record field label
   * @return the builder instance
   */
  public GradoopParquetTypeBuilder addGradoopIdField(String label) {
    this.messageTypeBuilder.addField(
      Types.required(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
        .length(GradoopId.ID_SIZE)
        .named(label)
    );
    return this;
  }

  /**
   * Adds a parquet type for a string field.
   *
   * @param label record field label
   * @return the builder instance
   */
  public GradoopParquetTypeBuilder addStringField(String label) {
    this.messageTypeBuilder.addField(
      Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named(label)
    );
    return this;
  }

  /**
   * Adds a parquet type for a properties field.
   *
   * @param label record field label
   * @return the builder instance
   */
  public GradoopParquetTypeBuilder addPropertiesField(String label) {
    this.messageTypeBuilder.addField(
      Types.optionalMap()
        .key(PrimitiveType.PrimitiveTypeName.BINARY)
          .as(LogicalTypeAnnotation.stringType())
        .requiredValue(PrimitiveType.PrimitiveTypeName.BINARY)
        .named(label)
    );
    return this;
  }

  /**
   * Adds a parquet type for a gradoop id set field.
   *
   * @param label record field label
   * @return the builder instance
   */
  public GradoopParquetTypeBuilder addGradoopIdSetField(String label) {
    this.messageTypeBuilder.addField(
      Types.requiredList()
        .requiredElement(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
        .length(GradoopId.ID_SIZE)
        .named(label)
    );
    return this;
  }

  /**
   * Adds a parquet type for a time interval (<code>Tuple2&lt;Long, Long&gt;</code>) field.
   *
   * @param label record field label
   * @return the builder instance
   */
  public GradoopParquetTypeBuilder addTimeInterval(String label) {
    this.messageTypeBuilder.addField(
      Types.requiredGroup()
        .addFields(
          Types.required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("from"),
          Types.required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("to")
        )
        .named(label)
    );
    return this;
  }

  /**
   * Creates the configured message type.
   *
   * @param name message type's name
   * @return configured message type
   */
  public MessageType build(String name) {
    return this.messageTypeBuilder.named(name);
  }
}
