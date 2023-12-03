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
package org.gradoop.flink.model.impl.operators.keyedgrouping.keys;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.functions.KeyFunction;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * A base class for key-function tests. This provides some common tests that should pass by all keys to be
 * used for grouping.
 *
 * @param <E> The element type.
 * @param <K> The key type.
 */
public abstract class KeyFunctionTestBase<E, K> extends GradoopFlinkTestBase {

  /**
   * Flag indicating whether a serialized and deserialized key should be checked for equality.
   * Setting this to false might be useful when testing composite keys or keys that use array types.
   */
  protected boolean checkForKeyEquality;

  /**
   * Get an instance of the key function to test.
   *
   * @return The key function.
   */
  protected abstract KeyFunction<E, K> getInstance();

  /**
   * Get a {@link Map} of test cases for the {@link KeyFunction#getKey(Object)} method.
   * Keys of this map will be elements and values keys extracted from those elements.<p>
   * The default implementation of this method provides no test elements.
   *
   * @return A map of test cases.
   */
  protected Map<E, K> getTestElements() {
    return Collections.emptyMap();
  }

  /**
   * Setup this test.
   */
  @Before
  public void setUp() {
    checkForKeyEquality = true;
  }

  /**
   * Check if the {@link KeyFunction#getType()} function returns a non-{@code null} value that is a valid
   * key type.
   */
  @Test
  public void checkTypeInfo() {
    TypeInformation<K> type = getInstance().getType();
    assertNotNull("Type information provided by the key fuction was null.", type);
    assertTrue("Type is not a valid key type.", type.isKeyType());
    assertNotEquals("Key type has no fields.", 0, type.getTotalFields());
  }

  /**
   * Check if a key is of a certain type and if it is serializable as that type.
   *
   * @param type The type.
   * @param key  The key to check.
   * @throws IOException when serialization of the key fails.
   */
  protected void checkKeyType(TypeInformation<K> type, K key) throws IOException {
    assertTrue(type.getTypeClass().isInstance(key));
    // Check serializability
    ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
    DataOutputViewStreamWrapper output = new DataOutputViewStreamWrapper(byteOutput);
    TypeSerializer<K> serializer = type.createSerializer(getExecutionEnvironment().getConfig());
    serializer.serialize(key, output);
    output.close();
    byte[] serializedData = byteOutput.toByteArray();
    DataInputViewStreamWrapper input = new DataInputViewStreamWrapper(
      new ByteArrayInputStream(serializedData));
    K deserializedKey = serializer.deserialize(input);
    if (checkForKeyEquality) {
      assertTrue(Objects.deepEquals(key, deserializedKey));
    }
    input.close();
  }

  /**
   * Test the {@link KeyFunction#getKey(Object)} function using test cases provided by
   * {@link #getTestElements()}.
   *
   * @throws IOException when serialization of a key fails
   */
  @Test
  public void testGetKey() throws IOException {
    final KeyFunction<E, K> function = getInstance();
    TypeInformation<K> type = function.getType();
    for (Map.Entry<E, K> testCase : getTestElements().entrySet()) {
      final K actual = function.getKey(testCase.getKey());
      final K expected = testCase.getValue();
      checkKeyType(type, actual);
      assertTrue(Objects.deepEquals(expected, actual));
    }
  }
}
