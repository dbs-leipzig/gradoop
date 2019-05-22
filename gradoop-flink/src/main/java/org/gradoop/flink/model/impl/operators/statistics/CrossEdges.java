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
package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

/**
 * Count number of crossing edges in a graph-layout
 */
public class CrossEdges implements UnaryGraphToValueOperator<DataSet<Tuple2<Integer, Double>>> {

  /** Default property for the x-coordinate of a vertex */
  public static final String DEFAULT_X_COORDINATE_PROPERTY = "X";
  /** Default property for the y-coordinate of a vertex */
  public static final String DEFAULT_Y_COORDINATE_PROPERTY = "Y";


  /**
   * wheather to ignore duplicate edges when counting crossings
   */
  private boolean ignoreDuplicates;

  /**
   * Property for the x-coordinate of a vertex
   */
  private String xCoordinateProperty;

  /**
   * Property for the y-coordinate of a vertex
   */
  private String yCoordinateProperty;

  /**
   * Create new CrossEdges-counter
   */
  public CrossEdges() {
    this(DEFAULT_X_COORDINATE_PROPERTY, DEFAULT_Y_COORDINATE_PROPERTY, true);
  }

  /**
   * Create new CrossEdges-counter
   *
   * @param xCoordinateProperty Property for the x-coordinate of a vertex. Default: X
   * @param yCoordinateProperty Property for the y-coordinate of a vertex. Default: Y
   */
  public CrossEdges(String xCoordinateProperty, String yCoordinateProperty) {
    this(xCoordinateProperty, yCoordinateProperty, true);
  }

  /**
   * Create new CrossEdges-counter
   *
   * @param xCoordinateProperty Property for the x-coordinate of a vertex. Default: X
   * @param yCoordinateProperty Property for the y-coordinate of a vertex. Default: Y
   * @param ignoreDuplicates    Ignore duplicate edges when counting crossings
   */
  public CrossEdges(String xCoordinateProperty, String yCoordinateProperty,
    boolean ignoreDuplicates) {
    this.xCoordinateProperty = xCoordinateProperty;
    this.yCoordinateProperty = yCoordinateProperty;
    this.ignoreDuplicates = ignoreDuplicates;
  }

  /**
   * Compute number of crossing edges for the given graph.
   *
   * @param g The graph to analyse. NEEDS to have properties (X,Y) for the position on EVERY
   *          vertex.
   * @return A single Tuple2. The first number is the total number of crossings. The second one
   * is the average number of crossings per edge.
   */
  @Override
  public DataSet<Tuple2<Integer, Double>> execute(LogicalGraph g) {
    DataSet<Edge> edges = g.getEdges();

    if (ignoreDuplicates) {
      edges = edges.map((MapFunction<Edge, Edge>) value -> {
        if (value.getTargetId().compareTo(value.getSourceId()) > 0) {
          GradoopId oldtarget = value.getTargetId();
          value.setTargetId(value.getSourceId());
          value.setSourceId(oldtarget);
        }
        return value;
      }).distinct("targetId", "sourceId");
    }

    DataSet<Integer> edgecountds = edges.map(x -> 1).reduce((a, b) -> a + b);

    edges = getEdgesWithCoordinates(edges, g.getVertices());
    DataSet<Integer> crosses = edges.cross(edges).with(
      (val1, val2) -> (val1.getId().compareTo(val2.getId()) > 0 && intersect(val1, val2)) ? 1 : 0)
      .reduce((a, b) -> a + b);

    return crosses.cross(edgecountds)
      .with((crosscount, edgecount) -> new Tuple2<>(crosscount, crosscount / (double) edgecount))
      .returns(new TypeHint<Tuple2<Integer, Double>>() {
      });
  }

  /**
   * Checks if two line-segments (x1,y1)->(x2,y2) and (x3,y3)->(x4,y4) intersect.
   * Currently cooccurence IS NOT considered as intersecting.
   * Also two lines sharing at least one start/end-point will be considered as non-intersecting
   *
   * @param x1 x-coordinate of starting-point of line-segment 1
   * @param y1 y-coordinate of starting-point of line-segment 1
   * @param x2 y-coordinate of end-point of line-segment 1
   * @param y2 y-coordinate of end-point of line-segment 1
   * @param x3 x-coordinate of starting-point of line-segment 2
   * @param y3 y-coordinate of starting-point of line-segment 2
   * @param x4 y-coordinate of end-point of line-segment 2
   * @param y4 y-coordinate of end-point of line-segment 2
   * @return true if both line-segments cross each other
   */
  private static boolean intersect(double x1, double y1, double x2, double y2, double x3, double y3,
    double x4, double y4) {
    if ((x1 == x3 && y1 == y3) || (x1 == x4 && y1 == y4) || (x2 == x3 && y2 == y3) ||
      (x2 == x4 && y2 == y4)) {
      return false;
    }
    double div = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4);
    if (Math.abs(div) < 0.001) {
      return false;
    }
    double t = ((x1 - x3) * (y3 - y4) - (y1 - y3) * (x3 - x4)) / div;
    double u = -(((x1 - x2) * (y1 - y3) - (y1 - y2) * (x1 - x3)) / div);
    return t > 0 && t < 1 && u > 0 && u < 1;
  }

  /**
   * Checks if two edges intersect.
   * Edge-coordinates are given as source_x, source_y, target_x and target_y properties of the
   * edge.
   * Currently cooccurence IS NOT considered as intersecting.
   * Also two lines sharing at least one start/end-point will be considered as non-intersecting
   *
   * @param e1 First edge
   * @param e2 Sedond edge
   * @return True if both edges cross each-other
   */
  protected static boolean intersect(Edge e1, Edge e2) {
    int x1 = e1.getPropertyValue("source_x").getInt();
    int y1 = e1.getPropertyValue("source_y").getInt();
    int x2 = e1.getPropertyValue("target_x").getInt();
    int y2 = e1.getPropertyValue("target_y").getInt();
    int x3 = e2.getPropertyValue("source_x").getInt();
    int y3 = e2.getPropertyValue("source_y").getInt();
    int x4 = e2.getPropertyValue("target_x").getInt();
    int y4 = e2.getPropertyValue("target_y").getInt();
    return intersect(x1, y1, x2, y2, x3, y3, x4, y4);
  }

  /**
   * Helper function to add coordinate properties to edges based on the coordinate-properties
   * of their vertices.
   *
   * @param edges    The edges to add coordinates to
   * @param vertices The vertices that supply the start/end-coordinates for the edges
   * @return The input edges with properties source_x, source_y, target_x, target_y
   * representing their coordinates
   */
  private DataSet<Edge> getEdgesWithCoordinates(DataSet<Edge> edges,
    DataSet<Vertex> vertices) {
    final String xproperty = xCoordinateProperty;
    final String yproperty = yCoordinateProperty;
    return edges.join(vertices).where("sourceId").equalTo("id").with((first, second) -> {
      first
        .setProperty("source_x", second.getPropertyValue(xproperty));
      first
        .setProperty("source_y", second.getPropertyValue(yproperty));
      return first;
    }).join(vertices).where("targetId").equalTo("id").with((first, second) -> {
      first
        .setProperty("target_x", second.getPropertyValue(xproperty));
      first
        .setProperty("target_y", second.getPropertyValue(yproperty));
      return first;
    });
  }
}
