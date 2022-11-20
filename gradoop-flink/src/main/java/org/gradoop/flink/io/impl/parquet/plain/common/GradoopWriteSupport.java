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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;

import javax.annotation.Nullable;
import java.util.Collections;

/**
 * Base class for all EPGM/TPGM parquet write supports
 *
 * @param <R> the record type
 */
public abstract class GradoopWriteSupport<R> extends WriteSupport<R> {

  /**
   * the current record consumer
   */
  private RecordConsumer recordConsumer;

  @Override
  public final WriteContext init(Configuration configuration) {
    return new WriteContext(this.getMessageType(), Collections.emptyMap());
  }

  @Override
  public final void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  @Override
  public final void write(R record) {
    this.recordConsumer.startMessage();
    this.writeRecord(record);
    this.recordConsumer.endMessage();
  }

  /**
   * Retrurns the record's message type
   *
   * @return the record's message type
   */
  public abstract MessageType getMessageType();

  /**
   * Gets called once per record.
   *
   * @param record the record to be written
   */
  public abstract void writeRecord(R record);

  /**
   * Appends a gradoop id field to the record consumer
   *
   * @param label record field label
   * @param index record field index
   * @param value the gradoop id
   */
  protected void writeGradoopId(String label, int index, GradoopId value) {
    this.recordConsumer.startField(label, index);
    this.recordConsumer.addBinary(Binary.fromConstantByteArray(value.toByteArray()));
    this.recordConsumer.endField(label, index);
  }

  /**
   * Appends a string field to the record consumer
   *
   * @param label record field label
   * @param index record field index
   * @param value the string
   */
  protected void writeString(String label, int index, String value) {
    this.recordConsumer.startField(label, index);
    this.recordConsumer.addBinary(Binary.fromString(value));
    this.recordConsumer.endField(label, index);
  }

  /**
   * Appends a properties field to the record consumer.
   *
   * @param label record field label
   * @param index record field index
   * @param value the properties
   */
  protected void writeProperties(String label, int index, @Nullable Properties value) {
    // skip empty properties since property fields are optional
    if (value == null || value.isEmpty()) {
      return;
    }

    this.recordConsumer.startField(label, index);
    this.recordConsumer.startGroup();

    this.recordConsumer.startField("key_value", 0);
    for (String propertyKey : value.getKeys()) {
      this.recordConsumer.startGroup();

      writeString("key", 0, propertyKey);

      this.recordConsumer.startField("value", 1);
      PropertyValue propertyValue = value.get(propertyKey);
      this.recordConsumer.addBinary(Binary.fromConstantByteArray(propertyValue.getRawBytes()));
      this.recordConsumer.endField("value", 1);

      this.recordConsumer.endGroup();
    }
    this.recordConsumer.endField("key_value", 0);

    this.recordConsumer.endGroup();
    this.recordConsumer.endField(label, index);
  }

  /**
   * Appends a gradoop id set field to the record consumer.
   *
   * @param label record field label
   * @param index record field index
   * @param value the gradoop id set
   */
  protected void writeGradoopIdSet(String label, int index, GradoopIdSet value) {
    this.recordConsumer.startField(label, index);
    this.recordConsumer.startGroup();

    this.recordConsumer.startField("list", 0);
    for (GradoopId element : value) {
      this.recordConsumer.startGroup();

      writeGradoopId("element", 0, element);

      this.recordConsumer.endGroup();
    }
    this.recordConsumer.endField("list", 0);

    this.recordConsumer.endGroup();
    this.recordConsumer.endField(label, index);
  }

  /**
   * Appends a time interval field to the record consumer.
   *
   * @param label record field label
   * @param index record field index
   * @param value the timer interval
   */
  protected void writeTimeIntervalField(String label, int index, Tuple2<Long, Long> value) {
    this.recordConsumer.startField(label, index);
    this.recordConsumer.startGroup();

    this.recordConsumer.startField("from", 0);
    this.recordConsumer.addLong(value.f0);
    this.recordConsumer.endField("from", 0);

    this.recordConsumer.startField("to", 1);
    this.recordConsumer.addLong(value.f1);
    this.recordConsumer.endField("to", 1);

    this.recordConsumer.endGroup();
    this.recordConsumer.endField(label, index);
  }
}
