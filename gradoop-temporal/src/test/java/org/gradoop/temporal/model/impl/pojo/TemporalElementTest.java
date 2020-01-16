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
package org.gradoop.temporal.model.impl.pojo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.temporal.model.api.TimeDimension;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

/**
 * Tests of class {@link TemporalElement}
 */
public class TemporalElementTest {
  /**
   * Test getter and setter.
   */
  @Test
  public void testConstructor() {
    GradoopId id = GradoopId.get();
    String label = "x";
    Properties props = Properties.create();
    props.set("k1", "v1");
    props.set("k2", "v2");
    Long validFrom = 42L;
    Long validTo = 52L;

    TemporalElement elementMock = mock(TemporalElement.class, withSettings()
      .useConstructor(id, label, props, validFrom, validTo)
      .defaultAnswer(CALLS_REAL_METHODS));

    assertSame(id, elementMock.getId());
    assertEquals(label, elementMock.getLabel());
    assertSame(props, elementMock.getProperties());
    assertEquals(validFrom, elementMock.getValidFrom());
    assertEquals(validTo, elementMock.getValidTo());
    assertTrue(elementMock.getTxFrom() <= System.currentTimeMillis());
    assertEquals(Long.MAX_VALUE, (long) elementMock.getTxTo());
  }

  /**
   * Test getter and setter.
   */
  @Test
  public void testGetterAndSetter() {
    GradoopId id = GradoopId.get();
    String label = "x";
    Properties props = Properties.create();
    props.set("k1", "v1");
    props.set("k2", "v2");
    Long validFrom = 42L;
    Long validTo = 52L;
    Long txFrom = 43L;
    Long txTo = 53L;

    TemporalElement elementMock = mock(TemporalElement.class, withSettings()
      .useConstructor(id, label, props, null, null)
      .defaultAnswer(CALLS_REAL_METHODS));

    elementMock.setValidFrom(validFrom);
    elementMock.setValidTo(validTo);

    assertSame(id, elementMock.getId());
    assertSame(props, elementMock.getProperties());
    assertEquals(validFrom, elementMock.getValidFrom());
    assertEquals(validTo, elementMock.getValidTo());
    assertTrue(elementMock.getTxFrom() <= System.currentTimeMillis());
    assertEquals((Long) Long.MAX_VALUE, elementMock.getTxTo());
    elementMock.setTxFrom(txFrom);
    elementMock.setTxTo(txTo);
    assertEquals(txFrom, elementMock.getTxFrom());
    assertEquals(txTo, elementMock.getTxTo());
  }

  /**
   * Test {@link TemporalElement#getTimeByDimension(TimeDimension)} with valid time dimension.
   */
  @Test
  public void testTimeGetterWithValidTimeDimension() {
    Long validFrom = 42L;
    Long validTo = 52L;

    TemporalElement elementMock = mock(TemporalElement.class, withSettings()
      .useConstructor(GradoopId.get(), null, null, null, null)
      .defaultAnswer(CALLS_REAL_METHODS));

    elementMock.setValidFrom(validFrom);
    elementMock.setValidTo(validTo);

    assertEquals(validFrom, elementMock.getTimeByDimension(TimeDimension.VALID_TIME).f0);
    assertEquals(validTo, elementMock.getTimeByDimension(TimeDimension.VALID_TIME).f1);
  }

  /**
   * Test {@link TemporalElement#getTimeByDimension(TimeDimension)} with tx time dimension.
   */
  @Test
  public void testTimeGetterWithTxTimeDimension() {
    Long txFrom = 42L;
    Long txTo = 52L;

    TemporalElement elementMock = mock(TemporalElement.class, withSettings()
      .useConstructor(GradoopId.get(), null, null, null, null)
      .defaultAnswer(CALLS_REAL_METHODS));

    elementMock.setTransactionTime(new Tuple2<>(txFrom, txTo));

    assertEquals(txFrom, elementMock.getTimeByDimension(TimeDimension.TRANSACTION_TIME).f0);
    assertEquals(txTo, elementMock.getTimeByDimension(TimeDimension.TRANSACTION_TIME).f1);
  }

  /**
   * Test {@link TemporalElement#getTimeByDimension(TimeDimension)} with null dimension.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testTimeGetterWithNullTimeDimension() {
    TemporalElement elementMock = mock(TemporalElement.class, withSettings()
      .useConstructor(GradoopId.get(), null, null, null, null)
      .defaultAnswer(CALLS_REAL_METHODS));

    elementMock.getTimeByDimension(null);
  }

  /**
   * Test {@link TemporalElement#setValidTime(Tuple2)} with an invalid interval.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testValidTimeSetterWithInvalidValues() {
    TemporalElement elementMock = mock(TemporalElement.class, withSettings()
      .useConstructor(GradoopId.get(), "", null, null, null)
      .defaultAnswer(CALLS_REAL_METHODS));
    elementMock.setValidTime(Tuple2.of(2L, 1L));
  }

  /**
   * Test {@link TemporalElement#setTransactionTime(Tuple2)} with an invalid interval.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testTransactionTimeSetterWithInvalidValues() {
    TemporalElement elementMock = mock(TemporalElement.class, withSettings()
      .useConstructor(GradoopId.get(), "", null, null, null)
      .defaultAnswer(CALLS_REAL_METHODS));
    elementMock.setTransactionTime(Tuple2.of(2L, 1L));
  }
}
