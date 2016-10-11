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
package org.gradoop.flink.io.impl.csv;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.functions.CSVToEdge;
import org.gradoop.flink.io.impl.csv.functions.CreateTuple;
import org.gradoop.flink.io.impl.csv.functions.CSVToVertex;
import org.gradoop.flink.io.impl.csv.pojos.Csv;
import org.gradoop.flink.io.impl.csv.pojos.Datasource;
import org.gradoop.flink.io.impl.csv.pojos.Domain;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;
import java.util.List;

public class CSVDataSource extends CSVBase implements DataSource {
  public final String CSV_VERTEX = "vertex";
  public final String CSV_EDGE = "edge";
  public final String CSV_GRAPH = "graph";
  private ExecutionEnvironment env;
  private String path;
  private final Datasource source;

  public CSVDataSource(GradoopFlinkConfig config, Datasource source,
    String path) {
    super(config, path);
    this.path = path;
    this.source = source;
    this.env = config.getExecutionEnvironment();
  }

  @Override
  public LogicalGraph getLogicalGraph() throws IOException {
    return null;
  }

  @Override
  public GraphCollection getGraphCollection() throws IOException {
    String datasourceName = source.getName();
    List<Domain> domains = source.getDomain();

    for (Domain domain : domains) {
      String domainName = domain.getName();
      for (Csv csv : domain.getCsv()) {
        if (csv.getType().equals(CSV_VERTEX)) {

          DataSet<String> vertexFile = env.readTextFile(path + csv.getName());
          DataSet<Csv> csvDataSet = env.fromElements(csv);

          DataSet<Tuple3<Csv, String, String>> vertexList = csvDataSet
            .cross(vertexFile)
            .with(new CreateTuple(domainName));

          DataSet<ImportVertex<String>> importVertices = vertexList
            .map(new CSVToVertex(datasourceName));

          try {
            importVertices.print();
          } catch (Exception e) {
            e.printStackTrace();
          }
        } else if (csv.getType().equals(CSV_EDGE)){

          DataSet<String> vertexFile = env.readTextFile(path + csv.getName());
          DataSet<Csv> csvDataSet = env.fromElements(csv);

          DataSet<Tuple2<Csv, String>> tupleList = csvDataSet
            .cross(vertexFile)
            .with(new CreateTuple());

          DataSet<ImportEdge<String>> importEdges = tupleList
            .map(new CSVToEdge(datasourceName, domainName));

        }
      }
    }

//    GraphDataSource<String> graphDataSource = new GraphDataSource<String>
//      (importVertices, null, null);

    return null;
  }

  @Override
  public GraphTransactions getGraphTransactions() throws IOException {
    return null;
  }
}
