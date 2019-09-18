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

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.layouting.LayoutingAlgorithm;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Count number of crossing edges in a graph-layout
 */
public class CrossEdges implements UnaryGraphToValueOperator<DataSet<Tuple2<Long, Double>>> {

  /**
   * Pass this value as cellSize to the Constructor to disable the anti-cross-product optimization.
   * In some cases this may lead to improved performance.
   */
  public static final int DISABLE_OPTIMIZATION = 0;

  /**
   * Property for the x-coordinate of a vertex
   */
  private String xCoordinateProperty;

  /**
   * Property for the y-coordinate of a vertex
   */
  private String yCoordinateProperty;
  /**
   * If true, ignore overlaps when counting crossings
   */
  private boolean ignoreOverlaps;

  /**
   * The size (width and height) of cells when dividing the coordinate-space into subcells to
   * speed if crossing-detection
   */
  private int cellSize;

  /**
   * Create new CrossEdges-counter. Coordinate-property-names are exected to match the ones
   * defined in LayoutingAlgorithm
   *
   * @param cellSize Size of sub-cells for crossing-detection-speedup. About 1/100th of the
   *                 *                    layouting-width is normally a good value. To disable
   *                 the optimization pass CrossEdges.DISABLE_OPTIMIZATION
   */
  public CrossEdges(int cellSize) {
    this(cellSize, LayoutingAlgorithm.X_COORDINATE_PROPERTY,
      LayoutingAlgorithm.Y_COORDINATE_PROPERTY);
  }

  /**
   * Create new CrossEdges-counter
   *
   * @param xCoordinateProperty Property for the x-coordinate of a vertex. Default: X
   * @param yCoordinateProperty Property for the y-coordinate of a vertex. Default: Y
   * @param cellSize            Size of sub-cells for crossing-detection-speedup. About 1/100th
   *                            of the
   *                            layouting-width is normally a good value. To disable
   *                            the optimization pass CrossEdges.DISABLE_OPTIMIZATION
   */
  public CrossEdges(int cellSize, String xCoordinateProperty, String yCoordinateProperty) {
    this.cellSize = cellSize;
    this.xCoordinateProperty = xCoordinateProperty;
    this.yCoordinateProperty = yCoordinateProperty;
  }

  /**
   * Sets optional value ignoreOverlaps. If true, do not count overlaps as crossings
   *
   * @param ignoreOverlaps the new value
   * @return this (for method-chaining)
   */
  public CrossEdges ignoreOverlaps(boolean ignoreOverlaps) {
    this.ignoreOverlaps = ignoreOverlaps;
    return this;
  }

  /**
   * Works just like execute() but performes the execution locally instead of using Flink.
   * For some reason this is A LOT faster. If your Dataset is small enough to be processed on a
   * single machine, USE THIS METHOD!
   * <p>
   * This method does ignore the cellSize value.
   *
   * @param g The graph to analyse. NEEDS to have properties (X,Y) for the position on EVERY
   *          vertex.
   * @return A single Tuple2. The first number is the total number of crossings. The second one
   * is the average number of crossings per edge.
   * @throws Exception A Flink-run is executed. Therefore exceptions might occur.
   */
  public Tuple2<Integer, Double> executeLocally(LogicalGraph g) throws Exception {
    DataSet<EPGMEdge> edges = g.getEdges();

    edges = removeSuperflousEdges(edges);

    List<Line> lines = getLinesFromEdges(edges, g.getVertices()).collect();

    int cores = Runtime.getRuntime().availableProcessors();
    ArrayList<Future<Integer>> results = new ArrayList<>(cores);

    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(cores);
    for (int core = 0; core < cores; core++) {
      final int len = lines.size() / cores;
      final int start = len * core;
      final int end = (core < cores - 1) ? (start + len) : lines.size();
      results.add(core, executor.submit(new Callable<Integer>() {
        private int crosscount = 0;

        @Override
        public Integer call() {
          for (int i = start; i < end; i++) {
            Line line1 = lines.get(i);
            for (int j = i + 1; j < lines.size(); j++) {
              Line line2 = lines.get(j);
              if (line1.intersects(line2)) {
                crosscount++;
              } else if (!ignoreOverlaps && line1.overlaps(line2)) {
                crosscount++;
              }
            }
          }
          return crosscount;
        }
      }));
    }

    int crosscount = 0;
    for (Future<Integer> f : results) {
      crosscount += f.get();
    }

    executor.shutdown();

    return new Tuple2<>(crosscount, crosscount / (double) lines.size());
  }

  /**
   * Removes all edges except for one between two given vertices. This is necessary for
   * undirected graphs as otherwise the cross-count would be incorrect
   *
   * @param edges The raw edges
   * @return The reduced edges
   */
  private DataSet<EPGMEdge> removeSuperflousEdges(DataSet<EPGMEdge> edges) {
    return edges.map((MapFunction<EPGMEdge, EPGMEdge>) value -> {
      if (value.getTargetId().compareTo(value.getSourceId()) < 0) {
        GradoopId oldtarget = value.getTargetId();
        value.setTargetId(value.getSourceId());
        value.setSourceId(oldtarget);
      }
      return value;
    }).distinct("sourceId", "targetId");
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
  public DataSet<Tuple2<Long, Double>> execute(LogicalGraph g) {
    DataSet<EPGMEdge> edges = g.getEdges();

    edges = removeSuperflousEdges(edges);

    DataSet<Long> edgecountds = edges.map(x -> 1L).reduce((a, b) -> a + b);

    DataSet<Line> lines = getLinesFromEdges(edges, g.getVertices());

    DataSet<Long> crosses;

    final boolean ignorOverlapsf = ignoreOverlaps;

    if (cellSize <= DISABLE_OPTIMIZATION) {
      crosses = lines.cross(lines).with(new CrossFunction<Line, Line, Long>() {
        @Override
        public Long cross(Line line1, Line line2) throws Exception {
          if (line1.getId().compareTo(line2.getId()) > 0) {
            if (line1.intersects(line2)) {
              return 1L;
            } else if (!ignorOverlapsf && line1.overlaps(line2)) {
              return 1L;
            }
          }
          return 0L;
        }
      }).reduce((a, b) -> a + b);
    } else {
      DataSet<Tuple2<Integer, Line>> segmentedLines = lines.flatMap(new LinePartitioner(cellSize));
      crosses = segmentedLines.join(segmentedLines).where(0).equalTo(0)
        .with(new JoinFunction<Tuple2<Integer, Line>, Tuple2<Integer, Line>, Long>() {
          @Override
          public Long join(Tuple2<Integer, Line> integerLineTuple1,
            Tuple2<Integer, Line> integerLineTuple2) throws Exception {
            Line line1 = integerLineTuple1.f1;
            Line line2 = integerLineTuple2.f1;
            if (line1.getId().compareTo(line2.getId()) > 0 && line1.intersects(line2)) {
              return 1L;
            }
            return 0L;
          }
        }).reduce((a, b) -> a + b);
    }

    return crosses.map(new RichMapFunction<Long, Tuple2<Long, Double>>() {
      @Override
      public Tuple2<Long, Double> map(Long crosscount) throws Exception {
        List<Long> edgecount = getRuntimeContext().getBroadcastVariable("edgecount");
        return new Tuple2<>(crosscount, crosscount / (double) edgecount.get(0));
      }
    }).withBroadcastSet(edgecountds, "edgecount");
  }

  /**
   * Convert edges and vertices (the vertices have coordinate-properties) to a dataset of lines.
   *
   * @param edges    The edges to generate lines from
   * @param vertices The vertices that supply the start/end-coordinates for the lines
   * @return A dataset of lines
   */
  private DataSet<Line> getLinesFromEdges(DataSet<EPGMEdge> edges, DataSet<EPGMVertex> vertices) {
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
    /**
     * size of the grid-cells
     */
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
      double midx = (line.getStart().getX() + line.getEnd().getX()) / 2;
      double midy = (line.getStart().getY() + line.getEnd().getY()) / 2;
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
      Line copy = l.copy();

      while (true) {
        Tuple2<Double, Double> intersection = firstGridIntersection(copy);
        if (intersection == null ||
          (intersection.f0 == copy.getEnd().getX() && intersection.f1 == copy.getEnd().getY())) {
          parts.add(copy);
          break;
        }
        Line segment =
          new Line(l.getId(), copy.getStart().getX(), copy.getStart().getY(), intersection.f0,
            intersection.f1);
        parts.add(segment);
        copy.getStart().setX(intersection.f0);
        copy.getStart().setY(intersection.f1);
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

      double dx = l.getEnd().getX() - l.getStart().getX();
      double dy = l.getEnd().getY() - l.getStart().getY();

      double gridLineX = closestMultipleInDirection(l.getStart().getX(), cellSize, dx);
      double gridLineY = closestMultipleInDirection(l.getStart().getY(), cellSize, dy);

      double xCrossT = (gridLineX - l.getStart().getX()) / dx;
      double yCrossT = (gridLineY - l.getStart().getY()) / dy;

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
        y = xCrossT * dy + l.getStart().getY();
      } else {
        x = yCrossT * dx + l.getStart().getX();
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

  /**
   * Pojo-Class to represent a line between two points
   */
  protected static class Line extends Tuple3<GradoopId, Vector, Vector> {
    /**
     * Create new line
     *
     * @param startX X-coordinate of starting-point
     * @param startY Y-coordinate of starting-point
     * @param endX   X-coordinate of end-point
     * @param endY   Y-coordinate of end-point
     */
    public Line(double startX, double startY, double endX, double endY) {
      super(GradoopId.NULL_VALUE, new Vector(startX, startY), new Vector(endX, endY));
    }

    /**
     * Create new line
     *
     * @param id     The Gradoop-id of the original edge
     * @param startX X-coordinate of starting-point
     * @param startY Y-coordinate of starting-point
     * @param endX   X-coordinate of end-point
     * @param endY   Y-coordinate of end-point
     */
    public Line(GradoopId id, double startX, double startY, double endX, double endY) {
      super(id, new Vector(startX, startY), new Vector(endX, endY));
    }

    /**
     * Create line from two points
     *
     * @param start start point
     * @param end   end point
     */
    public Line(Vector start, Vector end) {
      super(GradoopId.NULL_VALUE, start, end);
    }

    /**
     * Create line from two points
     *
     * @param id    id of line
     * @param start start point
     * @param end   end point
     */
    public Line(GradoopId id, Vector start, Vector end) {
      super(id, start, end);
    }

    /**
     * Default constructor to conform with POJO rules
     */
    public Line() {
      super(GradoopId.NULL_VALUE, new Vector(), new Vector());
    }

    /**
     * Deep Copy this line
     *
     * @return A copy of this line
     */
    public Line copy() {
      return new Line(f1.copy(), f2.copy());
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
      double startX = f1.getX();
      double startY = f1.getY();
      double endX = f2.getX();
      double endY = f2.getY();
      double otherstartX = other.f1.getX();
      double otherstartY = other.f1.getY();
      double otherendX = other.f2.getX();
      double otherendY = other.f2.getY();

      if ((startX == otherstartX && startY == otherstartY) ||
        (startX == otherendX && startY == otherendY) ||
        (endX == otherstartX && endY == otherstartY) || (endX == otherendX && endY == otherendY)) {
        return false;
      }
      double div =
        (startX - endX) * (otherstartY - otherendY) - (startY - endY) * (otherstartX - otherendX);
      if (Math.abs(div) < 0.001) {
        return false;
      }
      double t = ((startX - otherstartX) * (otherstartY - otherendY) -
        (startY - otherstartY) * (otherstartX - otherendX)) / div;
      double u =
        -(((startX - endX) * (startY - otherstartY) - (startY - endY) * (startX - otherstartX)) /
          div);
      return t > 0 && t < 1 && u > 0 && u < 1;
    }

    /**
     * Checks if two lines overlap (partially or completely)
     *
     * @param other The other line
     * @return True if they overlep, else false
     */
    public boolean overlaps(Line other) {
      //same start/end points
      if (other.isSame(this) || other.reverse().isSame(this)) {
        return true;
      }

      // must be parallel
      if (!other.isParallel(this)) {
        return false;
      }

      if (isPointOnLine(other.getStart()) || isPointOnLine(other.getEnd())) {
        return true;
      }

      if (other.isPointOnLine(getStart()) || other.isPointOnLine(getEnd())) {
        return true;
      }

      return false;
    }


    /**
     * Gets id
     *
     * @return value of id
     */
    public GradoopId getId() {
      return f0;
    }

    /**
     * Sets id
     *
     * @param id the new value
     */
    public void setId(GradoopId id) {
      f0 = id;
    }

    /**
     * Set the start point
     *
     * @param start the new point
     */
    public void setStart(Vector start) {
      f1 = start;
    }


    /**
     * Set the end point
     *
     * @param end the new point
     */
    public void setEnd(Vector end) {
      f2 = end;
    }

    /**
     * Get the start point
     *
     * @return start
     */
    public Vector getStart() {
      return f1;
    }

    /**
     * Get the end point
     *
     * @return end
     */
    public Vector getEnd() {
      return f2;
    }

    /**
     * Check if this line is exactly the same as another line (ignoring the id)
     *
     * @param other Other edge to compare this edge with
     * @return True if both are equal
     */
    public boolean isSame(Line other) {
      return getStart().equals(other.getStart()) && getEnd().equals(other.getEnd());
    }

    /**
     * Create a new line with swapped start- and endpoint
     *
     * @return A new line
     */
    public Line reverse() {
      return new Line(getId(), getEnd(), getStart());
    }

    /**
     * Check if two lines are parallel
     *
     * @param other The other line
     * @return True if parallel
     */
    public boolean isParallel(Line other) {
      Vector dir = getEnd().sub(getStart());
      Vector otherdir = other.getEnd().sub(other.getStart());
      return dir.normalized().scalar(otherdir.normalized()) > 0.99999;
    }

    /**
     * Check if the given point lies on this line
     *
     * @param point The point
     * @return True if yes
     */
    public boolean isPointOnLine(Vector point) {

      if (point.equals(getStart()) || point.equals(getEnd())) {
        return false;
      }

      Vector dir = getEnd().sub(getStart());
      Vector pointDir = point.sub(getStart());

      double xt = pointDir.getX() / dir.getX();
      double yt = pointDir.getY() / dir.getY();
      double dif = Math.abs(xt - yt);
      if (dif > 0.001 || Double.isNaN(dif)) {
        return false;
      }

      return xt > 0 && xt < 1;
    }

    @Override
    public String toString() {
      return "Line{" + "id=" + getId() + ", " + "start=" + getStart() + ", " + "end=" + getEnd() +
        "}";
    }
  }
}
