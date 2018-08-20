/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.sampling.evaluation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.EdgeCount;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;

/**
 * Computes the density of a graph and writes it to the graph head.
 * Uses: (|E|) / (|V| * (|V| - 1))
 */
public class GraphDensity implements UnaryGraphToGraphOperator {

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    VertexCount vc = new VertexCount();
    EdgeCount ec = new EdgeCount();
    DataSet<GraphHead> gh = graph.aggregate(vc).aggregate(ec).getGraphHead()
      .map(new MapFunction<GraphHead, GraphHead>() {
        @Override
        public GraphHead map(GraphHead graphHead) throws Exception {
          double vc1 = (double) graphHead.getPropertyValue("vertexCount").getLong();
          double ec1 = (double) graphHead.getPropertyValue("edgeCount").getLong();
          double density = ec1 / (vc1 * (vc1 - 1.));
          graphHead.setProperty(SamplingEvaluationConstants.PROPERTY_KEY_DENSITY, density);
          return graphHead;
        }
      });

    return graph.getConfig().getLogicalGraphFactory()
      .fromDataSets(gh, graph.getVertices(), graph.getEdges());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return GraphDensity.class.getName();
  }
}
