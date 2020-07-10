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

import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.pojo.ReservoirSampler;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ReservoirSamplerTest {

  @Test
  public void samplerTest() {
    int sampleSize = 100;
    ReservoirSampler<Integer> sampler = new ReservoirSampler<>(sampleSize);
    ArrayList<Integer> input = new ArrayList<>();
    for (int i = 0; i < 500; i++) {
      input.add(i);
    }

    assertTrue(sampler.updateSample(5));
    List<Integer> res1 = sampler.getReservoirSample();
    assertEquals(res1.size(), 1);
    assertEquals((int) res1.get(0), 5);

    sampler = new ReservoirSampler<>(sampleSize);
    sampler.updateSample(input);
    assertEquals(sampler.getSampleSize(), sampleSize);
    assertEquals(sampler.getSampleSize(), sampler.getReservoirSample().size());
    List<Integer> sample = sampler.getReservoirSample();
    for (int i = 0; i < sample.size(); i++) {
      assertTrue(sample.get(i) >= 0);
      assertTrue(sample.get(i) < 500);
      for (int j = 0; j < sample.size(); j++) {
        if (i == j) {
          continue;
        }
        assertNotEquals(sample.get(i), sample.get(j));
      }
    }
  }
}
