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

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.layouting.LayoutingAlgorithm;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

import java.util.List;

/**
 * Computes the standard-derivation of the edge-lengths of a layouted graph.
 * The better the layout is, the smaller the edge-length derivation. This metric is not as good
 * as CressEdges, but is a lot faster to compute.
 * <p>
 * All vertices of the input graph MUST have coordinates assigned as properties!
 */
public class EdgeLengthDerivation implements
  UnaryGraphToValueOperator<DataSet<Tuple2<Double, Double>>> {

  /**
   * Property for the x-coordinate of a vertex
   */
  private String xCoordinateProperty;

  /**
   * Property for the y-coordinate of a vertex
   */
  private String yCoordinateProperty;

  /**
   * Construct new EdgeLenghtDerivation-Statistic.
   *
   * @param xCoordinateProperty Property-name for the x-coordinates
   * @param yCoordinateProperty Property-name for the y-coordinates
   */
  public EdgeLengthDerivation(String xCoordinateProperty, String yCoordinateProperty) {
    this.xCoordinateProperty = xCoordinateProperty;
    this.yCoordinateProperty = yCoordinateProperty;
  }

  /**
   * Construct new EdgeLenghtDerivation-Statistic with default values (taken from
   * LayoutingAlgorithm) for the property-names.
   */
  public EdgeLengthDerivation() {
    this(LayoutingAlgorithm.X_COORDINATE_PROPERTY, LayoutingAlgorithm.Y_COORDINATE_PROPERTY);
  }

  /**
   * Calculate edge length derivation for the graph
   *
   * @param graph input graph
   * @return Tuple2. f0 is the standard derivation and f1 the normalized standard derivation.
   */
  @Override
  public DataSet<Tuple2<Double, Double>> execute(LogicalGraph graph) {
    DataSet<EPGMVertex> vertices = graph.getVertices();
    DataSet<EPGMEdge> edges = graph.getEdges();

    final String xCoordinatePropertyF = xCoordinateProperty;
    final String yCoordinatePropertyF = yCoordinateProperty;

    //get edge-lengths
    DataSet<Double> edgeLengths = edges.join(vertices).where("sourceId").equalTo("id")
      .with(new JoinFunction<EPGMEdge, EPGMVertex, EPGMEdge>() {
        public EPGMEdge join(EPGMEdge first, EPGMVertex second) throws Exception {
          first.setProperty("source_x", second.getPropertyValue(xCoordinatePropertyF));
          first.setProperty("source_y", second.getPropertyValue(yCoordinatePropertyF));
          return first;
        }
      }).join(vertices).where("targetId").equalTo("id")
      .with(new JoinFunction<EPGMEdge, EPGMVertex, Double>() {
        public Double join(EPGMEdge first, EPGMVertex second) throws Exception {
          Vector source = new Vector(first.getPropertyValue("source_x").getInt(),
            first.getPropertyValue("source_y").getInt());
          Vector target = new Vector(second.getPropertyValue(xCoordinatePropertyF).getInt(),
            second.getPropertyValue(yCoordinatePropertyF).getInt());
          return source.distance(target);
        }
      });

    // calculate average and count for later use
    DataSet<Tuple2<Integer, Double>> countAndAvg =
      edgeLengths.combineGroup(new GroupCombineFunction<Double, Tuple2<Integer, Double>>() {
        @Override
        public void combine(Iterable<Double> iterable,
          Collector<Tuple2<Integer, Double>> collector) throws Exception {
          int count = 0;
          double sum = 0;
          for (Double d : iterable) {
            count++;
            sum += d;
          }
          collector.collect(new Tuple2<>(count, sum));
        }
      }).reduce(new ReduceFunction<Tuple2<Integer, Double>>() {
        @Override
        public Tuple2<Integer, Double> reduce(Tuple2<Integer, Double> t1,
          Tuple2<Integer, Double> t2) throws Exception {
          t1.f0 += t2.f0;
          t1.f1 += t2.f1;
          return t1;
        }
      }).map(t -> {
        t.f1 = t.f1 / t.f0;
        return t;
      }).returns(new TypeHint<Tuple2<Integer, Double>>() {
      });

    // calculate standard-derivation using edge-lengths and the pre-computed statistics
    return edgeLengths.map(new RichMapFunction<Double, Double>() {
      private double avg;
      private int count;

      @Override
      public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        List<Tuple2<Integer, Double>> cntAvgL =
          this.getRuntimeContext().getBroadcastVariable("cntavg");
        avg = cntAvgL.get(0).f1;
        count = cntAvgL.get(0).f0;
      }

      @Override
      public Double map(Double d) {
        return Math.pow(d - avg, 2) / count;
      }

    }).withBroadcastSet(countAndAvg, "cntavg").reduce((a, b) -> a + b)
      .map(new RichMapFunction<Double, Tuple2<Double, Double>>() {
        @Override
        public Tuple2<Double, Double> map(Double v) {
          List<Tuple2<Integer, Double>> cntAvgL =
            this.getRuntimeContext().getBroadcastVariable("cntavg");
          double avg = cntAvgL.get(0).f1;
          double eld = Math.sqrt(v);
          return new Tuple2<Double, Double>(eld, eld / avg);
        }
      }).withBroadcastSet(countAndAvg, "cntavg");

  }
}
