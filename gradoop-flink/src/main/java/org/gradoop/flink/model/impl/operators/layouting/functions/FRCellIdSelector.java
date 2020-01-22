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

/**
 * A KeySelector that extracts the cellid of a Vertex.
 */
public class FRCellIdSelector implements KeySelector<LVertex, Integer> {

  /**
   * Type of neighbor to select if from
   */
  public enum NeighborType { UP, DOWN, LEFT, RIGHT, UPRIGHT, DOWNRIGHT, UPLEFT, DOWNLEFT, SELF }

  /**
   * Type of neighbor to get cellid from
   */
  private NeighborType type;

  /**
   * A KeySelector that extracts the cellid of a Vertex. (Or the cellid of one of it's neighbors)
   *
   * @param type Selects which id to return. The 'real' one or the id of a specific neighbor.
   */
  public FRCellIdSelector(NeighborType type) {
    this.type = type;
  }

  @Override
  public Integer getKey(LVertex value) {
    int cellid = value.getCellid();
    int xcell = cellid >> 16;
    int ycell = cellid & 0xFFFF;
    if (type == NeighborType.RIGHT || type == NeighborType.UPRIGHT ||
      type == NeighborType.DOWNRIGHT) {
      xcell++;
    }
    if (type == NeighborType.LEFT || type == NeighborType.DOWNLEFT || type == NeighborType.UPLEFT) {
      xcell--;
    }
    if (type == NeighborType.UP || type == NeighborType.UPLEFT || type == NeighborType.UPRIGHT) {
      ycell--;
    }
    if (type == NeighborType.DOWN || type == NeighborType.DOWNLEFT ||
      type == NeighborType.DOWNRIGHT) {
      ycell++;
    }

    return xcell << 16 | ycell;
  }
}
