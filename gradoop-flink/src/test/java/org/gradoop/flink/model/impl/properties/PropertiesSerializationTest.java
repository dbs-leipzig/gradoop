/**
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
package org.gradoop.flink.model.impl.properties;

import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GradoopFlinkTestUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PropertiesSerializationTest extends GradoopFlinkTestBase {

  @Test
  public void testPropertyValueSerialization() throws Exception {
    PropertyValue pIn = PropertyValue.create(10L);
    assertEquals("Property values were not equal", pIn,
      GradoopFlinkTestUtils.writeAndRead(pIn, getExecutionEnvironment()));
  }

  @Test
  public void testPropertySerialization() throws Exception {
    Property pIn = Property.create("key1", 10L);
    assertEquals("Properties were not equal", pIn,
      GradoopFlinkTestUtils.writeAndRead(pIn, getExecutionEnvironment()));
  }

  @Test
  public void testPropertyListSerialization() throws Exception {
    Properties pIn = Properties.createFromMap(
      GradoopTestUtils.SUPPORTED_PROPERTIES);
    assertEquals("Property Lists were not equal", pIn,
      GradoopFlinkTestUtils.writeAndRead(pIn, getExecutionEnvironment()));
  }
}
