/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.algorithms.fsm.gspan.decoders.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DFSStep;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Supportable<CompressedDfsCode> => (Graph, EPGMVertex, EPGMEdge)
 * @param <G> graph type
 */
public class DFSDecoder<G extends EPGMGraphHead> implements
  ResultTypeQueryable<Tuple3<G,
      ArrayList<Tuple2<GradoopId, Integer>>,
      ArrayList<Tuple3<GradoopId, GradoopId, Integer>>>>,
  MapFunction<WithCount<CompressedDFSCode>, Tuple3<G,
      ArrayList<Tuple2<GradoopId, Integer>>,
      ArrayList<Tuple3<GradoopId, GradoopId, Integer>>>> {

  /**
   * graph head factory
   */
  private final EPGMGraphHeadFactory<G> graphHeadFactory;
  /**
   * constructor
   * @param graphHeadFactory graph head factory
   */
  public DFSDecoder(EPGMGraphHeadFactory<G> graphHeadFactory) {
    this.graphHeadFactory = graphHeadFactory;
  }

  @Override
  public Tuple3<G, ArrayList<Tuple2<GradoopId, Integer>>,
    ArrayList<Tuple3<GradoopId, GradoopId, Integer>>> map(
    WithCount<CompressedDFSCode> compressedDfsCode) throws  Exception {

    DFSCode dfsCode = compressedDfsCode.getObject().getDfsCode();

    G graphHead = graphHeadFactory.createGraphHead();

    graphHead.setProperty("dfsCode", dfsCode.toString());
    graphHead.setProperty("support", compressedDfsCode.getCount());

    ArrayList<Tuple2<GradoopId, Integer>> vertices = Lists.newArrayList();

    ArrayList<Tuple3<GradoopId, GradoopId, Integer>> edges = Lists
      .newArrayListWithCapacity(dfsCode.getSteps().size());

    Map<Integer, GradoopId> vertexTimeId = new HashMap<>();

    for (DFSStep step : dfsCode.getSteps()) {

      Integer fromTime = step.getFromTime();
      Integer fromLabel = step.getFromLabel();

      Integer toTime = step.getToTime();
      Integer toLabel = step.getToLabel();

      GradoopId targetId;
      GradoopId sourceId;

      if (step.isOutgoing()) {
        sourceId = getOrCreateVertex(
          fromTime, fromLabel, vertices, vertexTimeId);

        targetId = getOrCreateVertex(
          toTime, toLabel, vertices, vertexTimeId);

      } else {
        sourceId = getOrCreateVertex(
          toTime, toLabel, vertices, vertexTimeId);

        if (step.isLoop()) {
          targetId = sourceId;
        } else {
          targetId = getOrCreateVertex(
            fromTime, fromLabel, vertices, vertexTimeId);
        }
      }

      edges.add(new Tuple3<>(sourceId, targetId, step.getEdgeLabel()));
    }

    return new Tuple3<>(graphHead, vertices, edges);
  }

  /**
   * returns the vertex id of a given time
   * or creates a new vertex if none exists.
   * @param time vertex time
   * @param label vertex label
   * @param vertices vertices
   * @param timeIdMap mapping : vertex time => vertex id
   * @return vertex id
   */
  private GradoopId getOrCreateVertex(Integer time, Integer label,
    ArrayList<Tuple2<GradoopId, Integer>> vertices,
    Map<Integer, GradoopId> timeIdMap) {

    GradoopId id = timeIdMap.get(time);

    if (id == null) {
      id = GradoopId.get();
      vertices.add(new Tuple2<>(id, label));
      timeIdMap.put(time, id);
    }
    return id;
  }

  @Override
  public TypeInformation<Tuple3<G, ArrayList<Tuple2<GradoopId, Integer>>,
    ArrayList<Tuple3<GradoopId, GradoopId, Integer>>>>
  getProducedType() {
    return new TupleTypeInfo<>(
      TypeExtractor.getForClass(graphHeadFactory.getType()),
      TypeExtractor.getForClass(ArrayList.class),
      TypeExtractor.getForClass(ArrayList.class)
    );
  }
}
