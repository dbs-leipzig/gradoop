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
import org.gradoop.common.model.impl.pojo.EPGMElement;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Base class for converting an {@link Element} into a protobuf object.
 *
 * @param <B> protobuf object type
 * @param <E> element type
 */
public abstract class ProtobufObjectToElement<B extends Message.Builder, E extends EPGMElement> implements
  MapFunction<B, E> {

  /**
   * Stores the properties for the {@link Element} to be converted.
   */
  private final Properties properties;

  /**
   * Constructor
   */
  public ProtobufObjectToElement() {
    this.properties = Properties.create();
  }

  /**
   * Converts a protobuf byte array ({@link ByteString}) into a gradoop id.
   *
   * @param value binary representation of a gradoop id
   * @return converted gradoop id
   */
  protected GradoopId parseGradoopId(ByteString value) {
    return GradoopId.fromByteArray(value.toByteArray());
  }

  /**
   * Converts a map of strings and binary property values ({@link ByteString}) into a property map.
   *
   * @param propertyValues map of strings and binary property values
   * @return converted properties
   */
  protected Properties parseProperties(Map<String, ByteString> propertyValues) {
    properties.clear();

    for (Map.Entry<String, ByteString> entry : propertyValues.entrySet()) {
      PropertyValue value = PropertyValue.fromRawBytes(entry.getValue().toByteArray());
      properties.set(entry.getKey(), value);
    }

    return properties;
  }

  /**
   * Converts a list of binary gradoop ids into a gradoop id set.
   *
   * @param gradoopIdList list of binary gradoop ids
   * @return converted gradoop id set
   */
  protected GradoopIdSet parseGradoopIds(List<ByteString> gradoopIdList) {
    List<GradoopId> gradoopIds = gradoopIdList.isEmpty() ? Collections.emptyList() :
      gradoopIdList.stream().map(this::parseGradoopId).collect(Collectors.toList());
    return GradoopIdSet.fromExisting(gradoopIds);
  }
}
