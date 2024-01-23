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

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Property;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class for converting an {@link Element} into a protobuf object.
 *
 * @param <E> element type
 * @param <B> protobuf builder type
 */
public abstract class ElementToProtobufObject<E extends Element, B extends Message.Builder>
  implements MapFunction<E, B> {

  /**
   * Singleton builder instance
   */
  private transient B builder;

  /**
   * Stores the properties for the protobuf object to be converted.
   */
  private final Map<String, ByteString> properties;

  /**
   * Stores the gradoop ids for the protobuf object to be converted.
   */
  private final List<ByteString> gradoopIds;

  /**
   * Constructor.
   */
  public ElementToProtobufObject() {
    this.properties = new HashMap<>();
    this.gradoopIds = new ArrayList<>();
  }

  /**
   * Creates a new builder instance.
   *
   * @return new builder
   */
  protected abstract B newBuilder();

  /**
   * Returns a reset builder. The builder instance is shared.
   *
   * @return reset builder
   */
  protected B resetAndGetBuilder() {
    if (this.builder == null) {
      this.builder = this.newBuilder();
    }
    this.builder.clear();
    return this.builder;
  }

  /**
   * Converts a gradoop id into a protobuf byte array ({@link ByteString}).
   *
   * @param value the gradoop id
   * @return converted protobuf byte array
   */
  protected ByteString serializeGradoopId(GradoopId value) {
    return ByteString.copyFrom(value.toByteArray());
  }

  /**
   * Converts the properties of an element into a map of strings and binary property values
   * ({@link ByteString}).
   *
   * @param element the element
   * @return converted map
   */
  protected Map<String, ByteString> serializeProperties(E element) {
    properties.clear();

    if (element.getPropertyCount() > 0) {
      for (Property property : element.getProperties()) {
        properties.put(property.getKey(), ByteString.copyFrom(property.getValue().getRawBytes()));
      }
    }

    return properties;
  }

  /**
   * Converts a gradoop id set into a list of binary gradoop ids ({@link ByteString}).
   *
   * @param gradoopIdSet the gradoop id set
   * @return converted list
   */
  protected List<ByteString> serializeGradoopIds(GradoopIdSet gradoopIdSet) {
    gradoopIds.clear();

    for (GradoopId gradoopId : gradoopIdSet) {
      gradoopIds.add(serializeGradoopId(gradoopId));
    }

    return gradoopIds;
  }
}
