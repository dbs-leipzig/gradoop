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
package org.gradoop.temporal.util;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.time.LocalDateTime;

/**
 * Test class of {@link TimeFormatConversion}.
 */
public class TimeFormatConversionTest {

  /**
   * Tests whether {@link TimeFormatConversion#toEpochMilli(LocalDateTime)} converts a given {@link LocalDateTime}
   * object to the correct number of milliseconds since Unix Epoch, if UTC is assumed.
   */
  @Test
  public void toEpochMilliTest() {
    LocalDateTime unixEpoch = LocalDateTime.of(1970, 1, 1, 0, 0);
    LocalDateTime oneTwoThree = LocalDateTime.of(2009, 2, 13, 23, 31, 30);

    AssertJUnit.assertEquals(1234567890000L, TimeFormatConversion.toEpochMilli(oneTwoThree));
    AssertJUnit.assertEquals(0L, TimeFormatConversion.toEpochMilli(unixEpoch));
  }

  /**
   * Tests whether {@link TimeFormatConversion#toLocalDateTime(long)} converts a given {@code long}
   * to the correct {@link LocalDateTime} object, if UTC is assumed.
   */
  @Test
  public void toLocalDateTimeTest() {
    LocalDateTime expectedUnixEpochZero = LocalDateTime.of(1970, 1, 1, 0, 0);
    LocalDateTime expectedOneTwoThree = LocalDateTime.of(2009, 2, 13, 23, 31, 30);
    long inputUnixEpochZero = 0L;
    long inputOneTwoThree = 1234567890000L;

    AssertJUnit.assertEquals(expectedUnixEpochZero, TimeFormatConversion.toLocalDateTime(inputUnixEpochZero));
    AssertJUnit.assertEquals(expectedOneTwoThree, TimeFormatConversion.toLocalDateTime(inputOneTwoThree));
  }
}
