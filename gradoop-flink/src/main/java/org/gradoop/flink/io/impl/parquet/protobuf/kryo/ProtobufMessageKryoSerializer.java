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
package org.gradoop.flink.io.impl.parquet.protobuf.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.protobuf.Message;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava18.com.google.common.cache.LoadingCache;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;

/**
 * Kryo Serializer for Protobuf {@link Message} objects.
 */
public class ProtobufMessageKryoSerializer extends Serializer<Message> {

  /**
   * Max cache size for {@link ProtobufMessageKryoSerializer#PARSE_FROM_METHOD_CACHE}
   */
  private static final int MAX_CACHE_SIZE = 100;

  /**
   * Cache for {@link Message} class to <code>parseFrom</code> method to skip redundant reflection api calls.
   */
  private static final LoadingCache<Class<Message>, Method> PARSE_FROM_METHOD_CACHE =
    CacheBuilder.newBuilder().maximumSize(MAX_CACHE_SIZE).build(CacheLoader.from(key -> {
      try {
        assert key != null;
        return key.getMethod("parseFrom", byte.class);
      } catch (NoSuchMethodException e) {
        throw new RuntimeException(String.format(
          "can't find parseFrom method for Protobuf message class: %s", key.getName()), e);
      }
    }));

  @Override
  public void write(Kryo kryo, Output output, Message object) {
    byte[] bytes = object.toByteArray();
    output.writeInt(bytes.length, true);
    output.writeBytes(bytes);
  }

  @Override
  public Message read(Kryo kryo, Input input, Class<Message> type) {
    try {
      int size = input.readInt(true);
      byte[] bytes = new byte[size];
      input.readBytes(bytes);

      return type.cast(PARSE_FROM_METHOD_CACHE.get(type).invoke(null, new Object[]{ bytes }));
    } catch (ExecutionException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("can't read protobuf message", e);
    }
  }
}
