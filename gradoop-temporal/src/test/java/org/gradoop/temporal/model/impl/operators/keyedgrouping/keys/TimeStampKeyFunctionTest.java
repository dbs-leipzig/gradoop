/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.keyedgrouping.keys;

import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.KeyFunctionWithDefaultValue;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.operators.keyedgrouping.TemporalGroupingKeys;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.testng.annotations.Test;

import java.time.temporal.ChronoField;
import java.time.temporal.TemporalField;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

/**
 * Test for the {@link TimeStampKeyFunction} key function.
 */
public class TimeStampKeyFunctionTest extends TemporalGradoopTestBase {

  /**
   * Test if default values are handled properly without a {@link TemporalField} set.
   */
  @Test
  public void testWithDefaultValueWithoutFieldExtraction() {
    TemporalElement element = getConfig().getTemporalGraphFactory().getVertexFactory().createVertex();
    element.setTxFrom(TemporalElement.DEFAULT_TIME_FROM);
    element.setTxTo(TemporalElement.DEFAULT_TIME_TO);
    KeyFunctionWithDefaultValue<TemporalElement, Long> function = TemporalGroupingKeys.timeStamp(
      TimeDimension.VALID_TIME, TimeDimension.Field.FROM);
    assertEquals(TemporalElement.DEFAULT_TIME_FROM, function.getKey(element));
    assertEquals(TemporalElement.DEFAULT_TIME_FROM, function.getDefaultKey());
    function = TemporalGroupingKeys.timeStamp(TimeDimension.VALID_TIME, TimeDimension.Field.TO);
    assertEquals(TemporalElement.DEFAULT_TIME_TO, function.getKey(element));
    assertEquals(TemporalElement.DEFAULT_TIME_TO, function.getDefaultKey());
    function = TemporalGroupingKeys.timeStamp(TimeDimension.TRANSACTION_TIME, TimeDimension.Field.FROM);
    assertEquals(TemporalElement.DEFAULT_TIME_FROM, function.getKey(element));
    assertEquals(TemporalElement.DEFAULT_TIME_FROM, function.getDefaultKey());
    function = TemporalGroupingKeys.timeStamp(TimeDimension.TRANSACTION_TIME, TimeDimension.Field.TO);
    assertEquals(TemporalElement.DEFAULT_TIME_TO, function.getKey(element));
    assertEquals(TemporalElement.DEFAULT_TIME_TO, function.getDefaultKey());
  }

  /**
   * Test if default values are handled properly with a {@link TemporalField} set.
   */
  @Test
  public void testWithDefaultValueWithFieldExtraction() {
    TemporalElement element = getConfig().getTemporalGraphFactory().getVertexFactory().createVertex();
    element.setTxFrom(TemporalElement.DEFAULT_TIME_FROM);
    element.setTxTo(TemporalElement.DEFAULT_TIME_TO);
    final Long defaultKey = -1L;
    KeyFunctionWithDefaultValue<TemporalElement, Long> function = TemporalGroupingKeys.timeStamp(
      TimeDimension.VALID_TIME, TimeDimension.Field.FROM, ChronoField.DAY_OF_MONTH);
    assertEquals(defaultKey, function.getKey(element));
    assertEquals(defaultKey, function.getDefaultKey());
    function = TemporalGroupingKeys.timeStamp(TimeDimension.VALID_TIME, TimeDimension.Field.TO,
      ChronoField.DAY_OF_MONTH);
    assertEquals(defaultKey, function.getKey(element));
    assertEquals(defaultKey, function.getDefaultKey());
    function = TemporalGroupingKeys.timeStamp(TimeDimension.TRANSACTION_TIME, TimeDimension.Field.FROM,
      ChronoField.DAY_OF_MONTH);
    assertEquals(defaultKey, function.getKey(element));
    assertEquals(defaultKey, function.getDefaultKey());
    function = TemporalGroupingKeys.timeStamp(TimeDimension.TRANSACTION_TIME, TimeDimension.Field.TO,
      ChronoField.DAY_OF_MONTH);
    assertEquals(defaultKey, function.getKey(element));
    assertEquals(defaultKey, function.getDefaultKey());
  }

  /**
   * Test if the key is extracted properly without default values.
   */
  @Test
  public void testGetKey() {
    TemporalElement element = getConfig().getTemporalGraphFactory().getVertexFactory().createVertex();
    element.setTxFrom(1L);
    element.setTxTo(2L);
    element.setValidFrom(3L);
    element.setValidTo(4L);
    KeyFunctionWithDefaultValue<TemporalElement, Long> function = TemporalGroupingKeys.timeStamp(
      TimeDimension.TRANSACTION_TIME, TimeDimension.Field.FROM);
    assertEquals((Long) 1L, function.getKey(element));
    function = TemporalGroupingKeys.timeStamp(TimeDimension.TRANSACTION_TIME, TimeDimension.Field.TO);
    assertEquals((Long) 2L, function.getKey(element));
    function = TemporalGroupingKeys.timeStamp(TimeDimension.VALID_TIME, TimeDimension.Field.FROM);
    assertEquals((Long) 3L, function.getKey(element));
    function = TemporalGroupingKeys.timeStamp(TimeDimension.VALID_TIME, TimeDimension.Field.TO);
    assertEquals((Long) 4L, function.getKey(element));
  }

  /**
   * Test if keys are set properly on elements.
   */
  @Test
  public void testAddKeyToElement() {
    VertexFactory<TemporalVertex> vf = getConfig().getTemporalGraphFactory().getVertexFactory();
    KeyFunctionWithDefaultValue<TemporalElement, Long> function = TemporalGroupingKeys.timeStamp(
      TimeDimension.VALID_TIME, TimeDimension.Field.FROM);
    TemporalElement element = vf.createVertex();
    function.addKeyToElement(element, TemporalElement.DEFAULT_TIME_FROM);
    assertEquals(PropertyValue.create(TemporalElement.DEFAULT_TIME_FROM),
      element.getPropertyValue("time_VALID_TIME_FROM"));
    function.addKeyToElement(element, -1L);
    assertEquals(PropertyValue.create(-1L), element.getPropertyValue("time_VALID_TIME_FROM"));
    element = vf.createVertex();
    function = TemporalGroupingKeys.timeStamp(TimeDimension.VALID_TIME, TimeDimension.Field.FROM,
      ChronoField.DAY_OF_MONTH);
    function.addKeyToElement(element, -1L); // -1L is the default, since a TemporalField is set
    assertNull(element.getPropertyValue("time_VALID_TIME_FROM_DayOfMonth"));
    function.addKeyToElement(element, 2L);
    assertEquals(PropertyValue.create(2L), element.getPropertyValue("time_VALID_TIME_FROM_DayOfMonth"));
  }
}
