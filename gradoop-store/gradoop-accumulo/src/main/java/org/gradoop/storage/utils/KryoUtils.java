/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.storage.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * kryo utils
 */
public final class KryoUtils {

  /**
   * kryo instance, should be thread safe
   */
  private static final ThreadLocal<Kryo> KRYO_POOLS;

  static {
    KRYO_POOLS = ThreadLocal.withInitial(() -> {
      Kryo kryo = new Kryo();
      kryo.setReferences(false);
      kryo.register(Collection.class);
      kryo.register(Map.class);
      return kryo;
    });
  }

  /**
   * load object from byte array
   *
   * @param encoded encoded byte array
   * @param type type template
   * @param <T> type template
   * @return object instance
   * @throws IOException io error
   */
  public static <T> T loads(
    byte[] encoded,
    Class<T> type
  ) throws IOException {
    try (
      ByteArrayInputStream content = new ByteArrayInputStream(encoded);
      Input input = new Input(content)
    ) {
      //noinspection unchecked
      return KRYO_POOLS.get().readObject(input, type);
    }
  }

  /**
   * write object to byte array
   *
   * @param data data instance to be encode
   * @return encrypted byte array
   * @throws IOException io error
   */
  public static byte[] dumps(Object data) throws IOException {
    try (
      ByteArrayOutputStream content = new ByteArrayOutputStream();
      Output output = new Output(content)
    ) {
      KRYO_POOLS.get().writeObject(output, data);
      output.flush();
      return content.toByteArray();
    }
  }
}
