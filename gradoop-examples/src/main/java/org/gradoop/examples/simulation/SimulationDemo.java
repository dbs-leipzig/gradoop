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

package org.gradoop.examples.simulation;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.impl.EPGMDatabase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.matching.simulation.dual.DualSimulation;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.GradoopFlinkConfig;

import java.util.concurrent.TimeUnit;

/**
 * Reads LDBC Social Network and executes a graph simulation.
 */
public class SimulationDemo implements ProgramDescription {

  /**
   * Query graph.
   */
  public static final String QUERY = "" +
    "(p1:person {gender=\"male\"})-[:knows]->(p2:person {gender=\"female\"})" +
    "(p1)-[:isLocatedIn]->(:city)-[:isPartOf]->(c:country {name=\"Germany\"})" +
    "(p2)-[:studyAt]->(:university)-[:isLocatedIn]->(:city)-[:isPartOf]->(c)";

  /**
   * File containing EPGM vertices.
   */
  public static final String VERTICES_JSON = "nodes.json";
  /**
   * File containing EPGM edges.
   */
  public static final String EDGES_JSON = "edges.json";
  /**
   * File containing EPGM graph heads.
   */
  public static final String GRAPHS_JSON = "graphs.json";

  /**
   * Runs the simulation.
   *
   * @param args args[0]: input dir, args[1]: output dir
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    String inputDir  = args[0];
    String outputDir = args[1];

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    GradoopFlinkConfig<GraphHeadPojo, VertexPojo, EdgePojo> gradoopConf =
      GradoopFlinkConfig.createDefaultConfig(env);

    EPGMDatabase<GraphHeadPojo, VertexPojo, EdgePojo> epgmDatabase =
      EPGMDatabase.fromJsonFile(
        inputDir + VERTICES_JSON,
        inputDir + EDGES_JSON,
        inputDir + GRAPHS_JSON,
        gradoopConf
      );

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> result =
      execute(epgmDatabase.getDatabaseGraph());

    result.writeAsJson(
      outputDir + VERTICES_JSON,
      outputDir + EDGES_JSON,
      outputDir + GRAPHS_JSON
    );

    System.out.println(String.format("Net runtime [s]: %d",
      env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS)));
  }

  /**
   * Executes dual simulation on the given logical graph.
   *
   * @param databaseGraph data graph
   * @return result match graph
   */
  private static LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> execute(
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> databaseGraph) {

    DualSimulation<GraphHeadPojo, VertexPojo, EdgePojo> op =
      new DualSimulation<>(QUERY);

    return op.execute(databaseGraph);
  }

  @Override
  public String getDescription() {
    return SimulationDemo.class.getName();
  }
}
