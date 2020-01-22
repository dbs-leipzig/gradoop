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

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.layouting.FusingFRLayouter;
import org.gradoop.flink.model.impl.operators.layouting.util.LEdge;
import org.gradoop.flink.model.impl.operators.layouting.util.LGraph;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts an LGraph to an EPGM Graph
 */
public class LGraphToEPGMMapper {

  /**
   * Build a new EPGM-Graph from the given LGraph, by converting LVertices to vertices 1:1.
   *
   * @param input    The original EPGMGraph, to use it's factory
   * @param layouted The LGraph to convert
   * @return The converted LGraph
   */
  public LogicalGraph buildSimplifiedGraph(LogicalGraph input, LGraph layouted) {
    DataSet<EPGMVertex> vertices = layouted.getVertices().map(LGraphToEPGMMapper::mapTpEPGMVertex);
    DataSet<EPGMEdge> edges = layouted.getEdges().map(LGraphToEPGMMapper::mapToEPGMEdge);
    return input.getFactory().fromDataSets(vertices, edges);
  }

  /**
   * Map LVertex to EPGMVertex.
   *
   * @param lv Input LVertex
   * @return Output EPGMVertex
   */
  protected static EPGMVertex mapTpEPGMVertex(LVertex lv) {
    EPGMVertex v = new EPGMVertex(lv.getId(), "vertex", Properties.create(), null);
    lv.getPosition().setVertexPosition(v);
    v.setProperty(FusingFRLayouter.VERTEX_SIZE_PROPERTY, lv.getCount());
    v.setProperty(FusingFRLayouter.SUB_ELEMENTS_PROPERTY,
      getSubelementListValue(lv.getSubVertices()));
    return v;
  }

  /**
   * Map LEdge to EPGMEdge.
   *
   * @param le Input LEdge
   * @return Output EPGMEdge
   */
  protected static EPGMEdge mapToEPGMEdge(LEdge le) {
    EPGMEdge e = new EPGMEdge(le.getId(), "edge", le.getSourceId(), le.getTargetId(), Properties.create(),
        null);
    e.setProperty(FusingFRLayouter.VERTEX_SIZE_PROPERTY, le.getCount());
    e.setProperty(FusingFRLayouter.SUB_ELEMENTS_PROPERTY, getSubelementListValue(le.getSubEdges()));
    return e;
  }

  /**
   * Helper function to convert the List of sub-elements into a comma seperated string
   * Gradoop (especially the CSVDataSink) seems to have trouble with lists of PropertyValues, so
   * this is the easies workaround
   *
   * @param ids List of GradoopIds
   * @return A comma seperated string of ids
   */
  protected static String getSubelementListValue(List<GradoopId> ids) {
    return ids.stream().map(GradoopId::toString).collect(Collectors.joining(","));
  }
}
