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
package org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.pojos;

import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.pojo.Binning;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class BinningTest {

  @Test
  public void simpleBinsTest() {
    ArrayList<Long> input = new ArrayList<>();
    for (int i = 0; i < 300; i++) {
      input.add((long) i);
    }

    Binning binning = new Binning(input, 100);
    Long[] bins = binning.getBins();
    assertEquals(bins.length, 100);
    assertEquals((long) bins[0], Long.MIN_VALUE);
    for (int i = 1; i < 100; i++) {
      assertEquals((long) bins[i], 3 * i - 1);
    }

  }

  @Test(expected = IllegalArgumentException.class)
  public void illegalArgumentTest() {
    ArrayList<Long> input = new ArrayList<>();
    for (int i = 0; i < 142; i++) {
      input.add((long) i);
    }
    Binning binning = new Binning(input, 100);
  }
}
