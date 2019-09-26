/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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

import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;

import static org.gradoop.temporal.util.TimeFormatConversion.toEpochMilli;

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

    Assert.assertEquals(1234567890000L, toEpochMilli(oneTwoThree));
    Assert.assertEquals(0L, toEpochMilli(unixEpoch));
  }
}
