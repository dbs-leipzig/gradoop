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
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

/**
 * Root parquet group converter for EPGM elements.
 *
 * @param <R> the record type
 */
public abstract class GradoopRootConverter<R> extends GroupConverter {

  /**
   * the current record to be read
   */
  protected R record;

  /**
   * Contains every registered converter by addressed field name.
   */
  protected final Map<String, Converter> converterMap = new HashMap<>();
  /**
   * Array of all converters in-order of the fields based on the given {@link MessageType} during converter
   * creation.
   */
  private final Converter[] converters;

  /**
   * Creates a new root converter based on the given record type ({@link MessageType}).
   *
   * @apiNote the {@link GradoopRootConverter<R>#initializeConverters} methods gets called before the
   * converter array is initialized and should be used to register every available field converter for the
   * request record type
   * @param requestedSchema the record type
   */
  public GradoopRootConverter(MessageType requestedSchema) {
    this.initializeConverters();

    this.converters = new Converter[requestedSchema.getFieldCount()];
    for (int i = 0; i < this.converters.length; i++) {
      String name = requestedSchema.getFieldName(i);

      Converter converter = this.converterMap.get(name);
      if (converter == null) {
        throw new NullPointerException("can't find converter for field: " + name);
      }

      this.converters[i] = converter;
    }
  }

  /**
   * Gets called once during construction to allow for the registration of field converters.
   */
  protected abstract void initializeConverters();

  /**
   * Creates a new record instance.
   *
   * @return the new record instance
   */
  protected abstract R createRecord();

  /**
   * Registers the converter for all fields with the given name.
   *
   * @param name the field name
   * @param converter the converter
   */
  protected void registerConverter(String name, Converter converter) {
    this.converterMap.put(name, converter);
  }

  /**
   * Returns the current record.
   *
   * @return the current record
   */
  public final R getCurrentRecord() {
    return this.record;
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return this.converters[fieldIndex];
  }

  @Override
  public void start() {
    this.record = this.createRecord();
  }

  @Override
  public void end() {
    // NOOP
  }

  /**
   * Parquet converter for gradoop ids
   */
  protected static class GradoopIdConverter extends PrimitiveConverter {

    /**
     * The consumer for the read gradoop id
     */
    private final Consumer<GradoopId> consumer;

    /**
     * Creates a converter for gradoop ids.
     *
     * @param consumer the read value consumer
     */
    public GradoopIdConverter(Consumer<GradoopId> consumer) {
      this.consumer = consumer;
    }

    @Override
    public void addBinary(Binary value) {
      GradoopId gradoopId = GradoopId.fromByteArray(value.getBytes());
      this.consumer.accept(gradoopId);
    }
  }

  /**
   * Parquet converter for strings
   */
  protected static class StringConverter extends PrimitiveConverter {

    /**
     * The consumer for the read string
     */
    private final Consumer<String> consumer;

    /**
     * Creates a converter for strings.
     *
     * @param consumer the read value consumer
     */
    public StringConverter(Consumer<String> consumer) {
      this.consumer = consumer;
    }

    @Override
    public void addBinary(Binary value) {
      String string = value.toStringUsingUTF8();
      this.consumer.accept(string);
    }
  }

  /**
   * Parquet converter for {@link PropertyValue}
   */
  protected static class PropertyValueConverter extends PrimitiveConverter {

    /**
     * The consumer for the read {@link PropertyValue}
     */
    private final Consumer<PropertyValue> consumer;

    /**
     * Creates a converter for {@link PropertyValue}
     *
     * @param consumer the read value consumer
     */
    public PropertyValueConverter(Consumer<PropertyValue> consumer) {
      this.consumer = consumer;
    }

    @Override
    public void addBinary(Binary value) {
      PropertyValue propertyValue = PropertyValue.fromRawBytes(value.getBytesUnsafe());
      this.consumer.accept(propertyValue);
    }
  }

  /**
   * Parquet converter for longs
   */
  private static class LongValueConverter extends PrimitiveConverter {

    /**
     * The consumer for the read longs
     */
    private final LongConsumer consumer;

    /**
     * Creates a converter for longs.
     *
     * @param consumer the read value consumer
     */
    LongValueConverter(LongConsumer consumer) {
      this.consumer = consumer;
    }

    @Override
    public void addLong(long value) {
      this.consumer.accept(value);
    }
  }

  /**
   * Parquet group converter for parquet spec compliant map key-value pairs
   */
  private static class KeyValueConverter extends GroupConverter {

    /**
     * Notifies the parent converter that the key-value pair got read.
     */
    private final Runnable notifier;

    /**
     * The key converter
     */
    private final Converter keyConverter;
    /**
     * The value converter
     */
    private final Converter valueConverter;

    /**
     * Creates a group converter for parquet spec compliant map key-value pairs.
     *
     * @param notifier the read completion notifier
     * @param keyConverter the key converter
     * @param valueConverter the value converter
     */
    KeyValueConverter(Runnable notifier, Converter keyConverter, Converter valueConverter) {
      this.notifier = notifier;
      this.keyConverter = keyConverter;
      this.valueConverter = valueConverter;
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      if (fieldIndex == 0) {
        return keyConverter;
      } else if (fieldIndex == 1) {
        return valueConverter;
      } else {
        throw new IndexOutOfBoundsException("key_value only consists of two fields: 'key', 'value'");
      }
    }

    @Override
    public void start() {
    }

    @Override
    public void end() {
      this.notifier.run();
    }
  }

  /**
   * Parquet group converter for {@link Properties}
   */
  protected static class PropertiesConverter extends GroupConverter {

    /**
     * The consumer for the read {@link Properties}
     */
    private final Consumer<Properties> consumer;
    /**
     * The key-value pair converter
     */
    private final Converter keyValueConverter;

    /**
     * The current properties
     */
    private Properties properties;
    /**
     * The latest key
     */
    private String key;
    /**
     * The latest value
     */
    private PropertyValue value;

    /**
     * Creates a group converter for {@link Properties}.
     *
     * @param consumer the read value consumer
     */
    public PropertiesConverter(Consumer<Properties> consumer) {
      this.consumer = consumer;

      this.keyValueConverter = new KeyValueConverter(() -> this.properties.set(this.key, this.value),
        new StringConverter(key -> this.key = key), new PropertyValueConverter(value -> this.value = value));
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      if (fieldIndex != 0) {
        throw new IndexOutOfBoundsException("properties only consists of single field: 'key_value'");
      }
      return this.keyValueConverter;
    }

    @Override
    public void start() {
      this.properties = new Properties();
    }

    @Override
    public void end() {
      this.consumer.accept(this.properties);
    }
  }

  /**
   * Parquet group converter for parquet spec compliant list elements
   */
  private static class ListElementConverter extends GroupConverter {

    /**
     * The element's value converter
     */
    private final Converter converter;

    /**
     * Creates a group converter for parquet spec compliant list elements
     *
     * @param converter the element's value converter
     */
    ListElementConverter(Converter converter) {
      this.converter = converter;
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      if (fieldIndex != 0) {
        throw new IndexOutOfBoundsException("list only consists of single field: 'element'");
      }
      return this.converter;
    }

    @Override
    public void start() {
    }

    @Override
    public void end() {
    }
  }

  /**
   * Parquet group converter for {@link GradoopIdSet}
   */
  protected static class GradoopIdSetConverter extends GroupConverter {

    /**
     * The consumer for the read {@link GradoopIdSet}
     */
    private final Consumer<GradoopIdSet> consumer;
    /**
     * The list element converter
     */
    private final Converter converter;

    /**
     * The current gradoop id set
     */
    private GradoopIdSet gradoopIdSet;

    /**
     * Creates a group converter for {@link GradoopIdSet}.
     *
     * @param consumer the read value consumer
     */
    public GradoopIdSetConverter(Consumer<GradoopIdSet> consumer) {
      this.consumer = consumer;

      this.converter = new ListElementConverter(
        new GradoopIdConverter(id -> this.gradoopIdSet.add(id)));
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      if (fieldIndex != 0) {
        throw new IndexOutOfBoundsException("graph_ids only consists of single field: 'list'");
      }
      return this.converter;
    }

    @Override
    public void start() {
      this.gradoopIdSet = new GradoopIdSet();
    }

    @Override
    public void end() {
      this.consumer.accept(this.gradoopIdSet);
    }
  }

  /**
   * Parquet group converter for gradoop time intervals
   */
  protected static class TimeIntervalConverter extends GroupConverter {

    /**
     * The consumer for the read time interval
     */
    private final Consumer<Tuple2<Long, Long>> consumer;

    /**
     * The from time converter
     */
    private final Converter fromConverter;
    /**
     * The to time converter
     */
    private final Converter toConverter;

    /**
     * The current time interval
     */
    private Tuple2<Long, Long> timeInterval;

    /**
     * Creates a group converter for gradoop time intervals
     *
     * @param consumer the read value consumer
     */
    public TimeIntervalConverter(Consumer<Tuple2<Long, Long>> consumer) {
      this.consumer = consumer;

      this.fromConverter = new LongValueConverter(value -> timeInterval.f0 = value);
      this.toConverter = new LongValueConverter(value -> timeInterval.f1 = value);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      if (fieldIndex == 0) {
        return fromConverter;
      } else if (fieldIndex == 1) {
        return toConverter;
      } else {
        throw new IndexOutOfBoundsException("time interval only consists of two fields: 'from', 'to'");
      }
    }

    @Override
    public void start() {
      this.timeInterval = new Tuple2<>();
    }

    @Override
    public void end() {
      this.consumer.accept(timeInterval);
    }
  }
}
