package org.gradoop.flink.model.impl.properties;

import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GradoopFlinkTestUtils;
import org.junit.Test;

import static org.gradoop.flink.model.impl.GradoopFlinkTestUtils.writeAndRead;
import static org.junit.Assert.assertEquals;

public class PropertiesSerializationTest extends GradoopFlinkTestBase {

  @Test
  public void testPropertyValueSerialization() throws Exception {
    PropertyValue pIn = PropertyValue.create(10L);
    assertEquals("Property Values were not equal", pIn,
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
    PropertyList pIn = PropertyList.createFromMap(
      GradoopTestUtils.SUPPORTED_PROPERTIES);
    assertEquals("Property Lists were not equal", pIn,
      GradoopFlinkTestUtils.writeAndRead(pIn, getExecutionEnvironment()));
  }
}
