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
package org.gradoop.flink.model.impl.operators.layouting.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.junit.Assert;
import org.junit.Test;

import static org.gradoop.flink.model.impl.operators.layouting.functions.Util.getDummyVertex;

public class FRCellTest {

  private int id(int x, int y) {
    return (x << 16) | y;
  }

  @Test
  public void testCellIdSelector() throws Exception {
    int cellSize = 10;
    KeySelector<LVertex, Integer> selfselector =
      new FRCellIdSelector(FRCellIdSelector.NeighborType.SELF);
    FRCellIdMapper mapper = new FRCellIdMapper(cellSize);

    Assert.assertEquals(0, (long) selfselector.getKey(mapper.map(getDummyVertex(0, 0))));
    Assert.assertEquals(id(9, 9), (long) selfselector.getKey(mapper.map(getDummyVertex(99, 98))));
    Assert.assertEquals(id(0, 9), (long) selfselector.getKey(mapper.map(getDummyVertex(0, 95))));

    KeySelector<LVertex, Integer> neighborslector =
      new FRCellIdSelector(FRCellIdSelector.NeighborType.RIGHT);
    Assert.assertEquals(id(1, 0), (long) neighborslector.getKey(getDummyVertex(id(0, 0))));
    Assert.assertEquals(id(6, 3), (long) neighborslector.getKey(getDummyVertex(id(5, 3))));
    Assert.assertEquals(id(10, 9), (long) neighborslector.getKey(getDummyVertex(id(9, 9))));

    neighborslector = new FRCellIdSelector(FRCellIdSelector.NeighborType.LEFT);
    Assert.assertEquals(id(-1, 0), (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(id(4, 3), (long) neighborslector.getKey(getDummyVertex(id(5, 3))));
    Assert.assertEquals(id(8, 9), (long) neighborslector.getKey(getDummyVertex(id(9, 9))));

    neighborslector = new FRCellIdSelector(FRCellIdSelector.NeighborType.UP);
    Assert.assertEquals(id(0, -1), (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(id(5, 2), (long) neighborslector.getKey(getDummyVertex(id(5, 3))));
    Assert.assertEquals(id(9, 8), (long) neighborslector.getKey(getDummyVertex(id(9, 9))));

    neighborslector = new FRCellIdSelector(FRCellIdSelector.NeighborType.DOWN);
    Assert.assertEquals(id(0, 1), (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(id(5, 4), (long) neighborslector.getKey(getDummyVertex(id(5, 3))));
    Assert.assertEquals(id(9, 10), (long) neighborslector.getKey(getDummyVertex(id(9, 9))));

    neighborslector = new FRCellIdSelector(FRCellIdSelector.NeighborType.UPRIGHT);
    Assert.assertEquals(id(1, -1), (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(id(6, 2), (long) neighborslector.getKey(getDummyVertex(id(5, 3))));
    Assert.assertEquals(id(10, 8), (long) neighborslector.getKey(getDummyVertex(id(9, 9))));

    neighborslector = new FRCellIdSelector(FRCellIdSelector.NeighborType.UPLEFT);
    Assert.assertEquals(id(-1, -1), (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(id(4, 2), (long) neighborslector.getKey(getDummyVertex(id(5, 3))));
    Assert.assertEquals(id(8, 8), (long) neighborslector.getKey(getDummyVertex(id(9, 9))));

    neighborslector = new FRCellIdSelector(FRCellIdSelector.NeighborType.DOWNLEFT);
    Assert.assertEquals(id(-1, 1), (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(id(4, 4), (long) neighborslector.getKey(getDummyVertex(id(5, 3))));
    Assert.assertEquals(id(8, 10), (long) neighborslector.getKey(getDummyVertex(id(9, 9))));

    neighborslector = new FRCellIdSelector(FRCellIdSelector.NeighborType.DOWNRIGHT);
    Assert.assertEquals(id(1, 1), (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(id(6, 4), (long) neighborslector.getKey(getDummyVertex(id(5, 3))));
    Assert.assertEquals(id(10, 10), (long) neighborslector.getKey(getDummyVertex(id(9, 9))));

  }
}
