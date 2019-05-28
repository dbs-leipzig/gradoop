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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import java.util.LinkedList;
import java.util.List;

/**
 * Count number of crossing edges in a graph-layout
 */
public class CrossEdgesNew implements UnaryGraphToValueOperator<DataSet<Tuple2<Integer, Double>>> {

  /**
   * Default property for the x-coordinate of a vertex
   */
  public static final String DEFAULT_X_COORDINATE_PROPERTY = "X";
  /**
   * Default property for the y-coordinate of a vertex
   */
  public static final String DEFAULT_Y_COORDINATE_PROPERTY = "Y";

  /**
   * Property for the x-coordinate of a vertex
   */
  private String xCoordinateProperty;

  /**
   * Property for the y-coordinate of a vertex
   */
  private String yCoordinateProperty;

  /**
   * The size (width and height) of cells when dividing the coordinate-space into subcells to
   * speed if crossing-detection
   */
  private int cellSize;

  /**
   * Create new CrossEdges-counter
   *
   * @param cellSize Size of sub-cells for crossing-detection-speedup. About 1/100th of the
   *                 *                    layouting-width is normally a good value.
   */
  public CrossEdgesNew(int cellSize) {
    this(cellSize, DEFAULT_X_COORDINATE_PROPERTY, DEFAULT_Y_COORDINATE_PROPERTY);
  }

  /**
   * Create new CrossEdges-counter
   *
   * @param xCoordinateProperty Property for the x-coordinate of a vertex. Default: X
   * @param yCoordinateProperty Property for the y-coordinate of a vertex. Default: Y
   * @param cellSize            Size of sub-cells for crossing-detection-speedup. About 1/100th
   *                            of the
   *                            layouting-width is normally a good value.
   */
  public CrossEdgesNew(int cellSize, String xCoordinateProperty, String yCoordinateProperty) {
    this.cellSize = cellSize;
    this.xCoordinateProperty = xCoordinateProperty;
    this.yCoordinateProperty = yCoordinateProperty;
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


    edges = edges.map((MapFunction<Edge, Edge>) value -> {
      if (value.getTargetId().compareTo(value.getSourceId()) > 0) {
        GradoopId oldtarget = value.getTargetId();
        value.setTargetId(value.getSourceId());
        value.setSourceId(oldtarget);
      }
      return value;
    }).distinct("targetId", "sourceId");


    DataSet<Integer> edgecountds = edges.map(x -> 1).reduce((a, b) -> a + b);

    DataSet<Line> lines = getLinesFromEdges(edges, g.getVertices());

    DataSet<Tuple2<Integer, Line>> segmentedLines = lines.flatMap(new LinePartitioner(cellSize));

    DataSet<Integer> crosses = segmentedLines.join(segmentedLines).where(0).equalTo(0)
      .with(new JoinFunction<Tuple2<Integer, Line>, Tuple2<Integer, Line>, Integer>() {
        @Override
        public Integer join(Tuple2<Integer, Line> integerLineTuple1,
          Tuple2<Integer, Line> integerLineTuple2) throws Exception {
          Line line1 = integerLineTuple1.f1;
          Line line2 = integerLineTuple2.f1;
          if (line1.getId().compareTo(line2.getId()) > 0 && line1.intersects(line2)) {
            return 1;
          }
          return 0;
        }
      }).reduce((a, b) -> a + b);

    return crosses.cross(edgecountds)
      .with((crosscount, edgecount) -> new Tuple2<>(crosscount, crosscount / (double) edgecount))
      .returns(new TypeHint<Tuple2<Integer, Double>>() {
      });
  }

  /**
   * Convert edges and vertices (the vertices have coordinate-properties) to a dataset of lines.
   *
   * @param edges    The edges to generate lines from
   * @param vertices The vertices that supply the start/end-coordinates for the lines
   * @return A dataset of lines
   */
  private DataSet<Line> getLinesFromEdges(DataSet<Edge> edges, DataSet<Vertex> vertices) {
    final String xproperty = xCoordinateProperty;
    final String yproperty = yCoordinateProperty;
    return edges.join(vertices).where("sourceId").equalTo("id").with((first, second) -> {
      first.setProperty("source_x", second.getPropertyValue(xproperty));
      first.setProperty("source_y", second.getPropertyValue(yproperty));
      return first;
    }).join(vertices).where("targetId").equalTo("id").with(
      (first, second) -> new Line(first.getId(), first.getPropertyValue("source_x").getInt(),
        first.getPropertyValue("source_y").getInt(), second.getPropertyValue(xproperty).getInt(),
        second.getPropertyValue(yproperty).getInt()));
  }

  /**
   * Partitions the layouting-space into sub-cells to speed up crossing detection.
   * The Layouting-Space is divided into equal-sized subcells. All lines crossing more then one
   * subcell are split up into a number of sub-lines so that each sub-line is completely
   * contained in one subcell.
   * After this, crossings can be determined just by comparing the sub-lines inside each single
   * sub-cell.
   * <p>
   * Also each outputted sub-line is tagget with the id of the sub-cell it belongs to. (This way
   * sub-lines of the same sub-cell can be joined together)
   */
  protected static class LinePartitioner implements FlatMapFunction<Line, Tuple2<Integer, Line>> {
    /** size of the grid-cells */
    private int cellSize;

    /**
     * Create Partitioner
     *
     * @param cellSize Size of subcells
     */
    public LinePartitioner(int cellSize) {
      this.cellSize = cellSize;
    }

    @Override
    public void flatMap(Line line, Collector<Tuple2<Integer, Line>> collector) throws Exception {
      List<Line> segments = subdivideByGrid(line);
      for (Line l : segments) {
        collector.collect(new Tuple2<>(getCellId(l), l));
      }
    }

    /**
     * Generates a cellid for the subcell of the given line. All lines sharing the same sub-cell
     * get the same cellid.
     *
     * @param line The line
     * @return An id.
     */
    public Integer getCellId(Line line) {
      double midx = (line.startX + line.endX) / 2;
      double midy = (line.startY + line.endY) / 2;
      return ((int) (midx / cellSize) << 16) | (int) (midy / cellSize);
    }

    /**
     * Uses a grid of cells to divide a line into a list of smaller sub-lines. Each sub-line is
     * competely contained in one grid-cell
     *
     * @param l The line to sub-divide
     * @return A list of lines
     */
    public List<Line> subdivideByGrid(Line l) {
      List<Line> parts = new LinkedList<>();
      Line copy = l.clone();

      while (true) {
        Tuple2<Double, Double> intersection = firstGridIntersection(copy);
        if (intersection == null ||
          (intersection.f0 == copy.endX && intersection.f1 == copy.endY)) {
          parts.add(copy);
          break;
        }
        Line segment = new Line(l.id, copy.startX, copy.startY, intersection.f0, intersection.f1);
        parts.add(segment);
        copy.startX = intersection.f0;
        copy.startY = intersection.f1;
      }

      return parts;
    }

    /**
     * Returns the first intersection between the given line and the grid
     *
     * @param l The line
     * @return A tuple of x and y coordinates. Returns null if the line does not intersect with the
     * grid.
     */
    protected Tuple2<Double, Double> firstGridIntersection(Line l) {

      double dx = l.endX - l.startX;
      double dy = l.endY - l.startY;

      double gridLineX = closestMultipleInDirection(l.startX, cellSize, dx);
      double gridLineY = closestMultipleInDirection(l.startY, cellSize, dy);

      double xCrossT = (gridLineX - l.startX) / dx;
      double yCrossT = (gridLineY - l.startY) / dy;

      if (Double.isNaN(xCrossT)) {
        xCrossT = Double.MAX_VALUE;
      }

      if (Double.isNaN(yCrossT)) {
        yCrossT = Double.MAX_VALUE;
      }

      if (xCrossT > 1 && yCrossT > 1) {
        return null;
      }

      double x;
      double y;

      if (Math.abs(xCrossT - yCrossT) < 0.0000001) {
        x = gridLineX;
        y = gridLineY;
      } else if (Math.abs(xCrossT) < Math.abs(yCrossT)) {
        x = gridLineX;
        y = xCrossT * dy + l.startY;
      } else {
        x = yCrossT * dx + l.startX;
        y = gridLineY;
      }

      return new Tuple2<>(x, y);
    }

    /**
     * Helper-method
     * Returns the number that is the closest multiple of base from number in the given direction
     *
     * @param number    The reference-number
     * @param base      The base for the calculated multiple
     * @param direction The direction in which to find the multiple. (>0 or <0)
     * @return The result.
     */
    private static Double closestMultipleInDirection(double number, double base, double direction) {
      if (number % base == 0) {
        return number + base * Math.signum(direction);
      }
      if (direction > 0) {
        return Math.ceil(number / base) * base;
      } else {
        return Math.floor(number / base) * base;
      }
    }
  }

  /** Pojo-Class to represent a line between two points
   *
   */
  protected static class Line implements Cloneable {
    /** The GradoopId of the original edge */
    private GradoopId id;
    /** X-coordinate of starting-point */
    private double startX;
    /** Y-coordinate of starting-point */
    private double startY;
    /** X-coordinate of end-point */
    private double endX;
    /** Y-coordinate of end-point */
    private double endY;

    /** Create new line
     *
     * @param startX X-coordinate of starting-point
     * @param startY Y-coordinate of starting-point
     * @param endX X-coordinate of end-point
     * @param endY Y-coordinate of end-point
     */
    public Line(double startX, double startY, double endX, double endY) {
      this.startX = startX;
      this.startY = startY;
      this.endX = endX;
      this.endY = endY;
    }

    /** Create new line
     *
     * @param id The Gradoop-id of the original edge
     * @param startX X-coordinate of starting-point
     * @param startY Y-coordinate of starting-point
     * @param endX X-coordinate of end-point
     * @param endY Y-coordinate of end-point
     */
    public Line(GradoopId id, double startX, double startY, double endX, double endY) {
      this.id = id;
      this.startX = startX;
      this.startY = startY;
      this.endX = endX;
      this.endY = endY;
    }

    /** Clone this line
     *
     * @return A copy of this line
     */
    public Line clone() {
      try {
        return (Line) super.clone();
      } catch (CloneNotSupportedException e) {
        //Can not happen
      }
      return null;
    }

    /**
     * Checks if this line and the other line intersect
     * Currently cooccurence IS NOT considered as intersecting.
     * Also two lines sharing at least one start/end-point will be considered as non-intersecting
     *
     * @param other The other line
     * @return true if both line-segments cross each other
     */
    public boolean intersects(Line other) {
      if ((startX == other.startX && startY == other.startY) ||
        (startX == other.endX && startY == other.endY) ||
        (endX == other.startX && endY == other.startY) ||
        (endX == other.endX && endY == other.endY)) {
        return false;
      }
      double div = (startX - endX) * (other.startY - other.endY) -
        (startY - endY) * (other.startX - other.endX);
      if (Math.abs(div) < 0.001) {
        return false;
      }
      double t = ((startX - other.startX) * (other.startY - other.endY) -
        (startY - other.startY) * (other.startX - other.endX)) / div;
      double u =
        -(((startX - endX) * (startY - other.startY) - (startY - endY) * (startX - other.startX)) /
          div);
      return t > 0 && t < 1 && u > 0 && u < 1;
    }


    /**
     * Gets id
     *
     * @return value of id
     */
    public GradoopId getId() {
      return id;
    }

    /**
     * Sets id
     *
     * @param id the new value
     */
    public void setId(GradoopId id) {
      this.id = id;
    }

    /**
     * Gets startX
     *
     * @return value of startX
     */
    public double getStartX() {
      return startX;
    }

    /**
     * Sets startX
     *
     * @param startX the new value
     */
    public void setStartX(double startX) {
      this.startX = startX;
    }

    /**
     * Gets startY
     *
     * @return value of startY
     */
    public double getStartY() {
      return startY;
    }

    /**
     * Sets startY
     *
     * @param startY the new value
     */
    public void setStartY(double startY) {
      this.startY = startY;
    }

    /**
     * Gets endX
     *
     * @return value of endX
     */
    public double getEndX() {
      return endX;
    }

    /**
     * Sets endX
     *
     * @param endX the new value
     */
    public void setEndX(double endX) {
      this.endX = endX;
    }

    /**
     * Gets endY
     *
     * @return value of endY
     */
    public double getEndY() {
      return endY;
    }

    /**
     * Sets endY
     *
     * @param endY the new value
     */
    public void setEndY(double endY) {
      this.endY = endY;
    }

    @Override
    public String toString() {
      return "Line{" + "id=" + id + ", startX=" + startX + ", startY=" + startY + ", endX=" + endX +
        ", endY=" + endY + '}';
    }
  }
}
