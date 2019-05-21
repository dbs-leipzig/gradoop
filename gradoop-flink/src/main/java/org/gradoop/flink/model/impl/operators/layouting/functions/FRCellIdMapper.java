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
package org.gradoop.flink.model.impl.operators.layouting.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.layouting.FRLayouter;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

/**
 * A map-function that sssigns a cellid to each input-vertex, depending on its position in the
 * layouting-space.
 * The cellid is stored as a property in FRLayouter.CELLID_PROPERTY
 */
public class FRCellIdMapper implements MapFunction<Vertex, Vertex> {
  /** Number of cells per axis */
  private int cellResolution;
  /** Width of the layouting-space */
  private int width;
  /** Height of the layouting-space */
  private int height;

  /** Create new CellIdMapper
   * @param cellResolution Number of cells per axis
   * @param width          width of the layouting-space
   * @param height         height of the layouting-space
   */
  public FRCellIdMapper(int cellResolution, int width, int height) {
    this.cellResolution = cellResolution;
    this.width = width;
    this.height = height;
  }

  @Override
  public Vertex map(Vertex value) {
    Vector pos = Vector.fromVertexPosition(value);
    int xcell = ((int) pos.getX()) / (width / cellResolution);
    int ycell = ((int) pos.getY()) / (height / cellResolution);
    int cellid = ycell * cellResolution + xcell;
    value.setProperty(FRLayouter.CELLID_PROPERTY, cellid);
    return value;
  }
}
