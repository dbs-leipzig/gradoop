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
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.functions.*;
import org.gradoop.flink.io.impl.csv.pojos.Csv;
import org.gradoop.flink.io.impl.csv.pojos.Datasource;
import org.gradoop.flink.io.impl.csv.pojos.Edge;
import org.gradoop.flink.io.impl.csv.pojos.Graphhead;
import org.gradoop.flink.io.impl.csv.pojos.Vertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class CSVDataSource extends CSVBase implements DataSource {
  public static final String CSV_VERTEX = "vertex";
  public static final String CSV_EDGE = "edge";
  public static final String CSV_GRAPH = "graph";
  private ExecutionEnvironment env;
  private String path;
  private final DataSet<Datasource> datasource;

  private GraphHeadFactory graphHeadFactory;
  private VertexFactory vertexFactory;
  private EdgeFactory edgeFactory;


  public CSVDataSource(GradoopFlinkConfig config, Datasource source,
    String path) {
    super(config, path);
    this.path = path;
    this.env = config.getExecutionEnvironment();
    datasource = env.fromElements(source);

    graphHeadFactory = config.getGraphHeadFactory();
    vertexFactory = config.getVertexFactory();
    edgeFactory = config.getEdgeFactory();
  }

  @Override
  public LogicalGraph getLogicalGraph() throws IOException {
    return null;
  }

  @Override
  public GraphCollection getGraphCollection() throws IOException {
    DataSet<Csv> csv = datasource.flatMap(new DatasourceToCsv());

    DataSet<org.gradoop.common.model.impl.pojo.GraphHead> graphHeads;
    DataSet<org.gradoop.common.model.impl.pojo.Vertex> vertices;
    DataSet<org.gradoop.common.model.impl.pojo.Edge> edges;

    Collection<Tuple2<Csv, List<String>>> content = new HashSet<>();
    try {
      for (Csv csvFile : csv.collect()) {
        List<String> lines = env.readTextFile(path + csvFile.getName())
          .collect();
        content.add(new Tuple2<Csv, List<String>>(csvFile, lines));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    DataSet<Tuple2<Csv, List<String>>> csvContent = env
      .fromCollection(content);

    DataSet<EPGMElement> elements =csvContent
      .flatMap(new CSVToElement(graphHeadFactory, vertexFactory,
        edgeFactory));

    graphHeads = csvContent
      .filter(
        new CSVTypeFilter(Graphhead.class))
      .flatMap(new CSVToElement(graphHeadFactory, vertexFactory,
        edgeFactory))
      .map(new EPGMElementToPojo<GraphHead>())
      .returns(graphHeadFactory.getType());

    vertices = csvContent
      .filter(
        new CSVTypeFilter(Vertex.class))
      .flatMap(new CSVToElement(graphHeadFactory, vertexFactory,
        edgeFactory))
      .map(new EPGMElementToPojo<org.gradoop.common.model.impl.pojo.Vertex>())
      .returns(vertexFactory.getType());

    DataSet<Map<String, GradoopId>> vertexIds = vertices
      .map(new VertexToVertexIds())
      .reduceGroup(new VertexIdsToMap());

    edges = csvContent
      .filter(
        new CSVTypeFilter(Edge.class))
      .flatMap(new CSVToElement(graphHeadFactory, vertexFactory, edgeFactory))
      .map(new EPGMElementToPojo<org.gradoop.common.model.impl.pojo.Edge>())
      .returns(edgeFactory.getType())
      .map(new GradoopEdgeIds())
      .withBroadcastSet(vertexIds, GradoopEdgeIds.ID_MAP);

    DataSet<Tuple2<org.gradoop.common.model.impl.pojo.Vertex, String>> vertexGraphKeys = vertices
      .flatMap(new ElementToElementGraphKey<org.gradoop.common.model.impl.pojo.Vertex>());

    DataSet<Tuple2<org.gradoop.common.model.impl.pojo.Edge, String>>
      edgeGraphKeys = edges
      .flatMap(new ElementToElementGraphKey<org.gradoop.common.model.impl
        .pojo.Edge>());

    graphHeads = graphHeads
      .union(vertexGraphKeys
        .groupBy(1)
        .reduceGroup(new ElementGraphKeyToGraphHead<org.gradoop.common.model.impl.pojo.Vertex>(graphHeadFactory)))
      .union(edgeGraphKeys
        .groupBy(1)
        .reduceGroup(new ElementGraphKeyToGraphHead<org.gradoop.common.model.impl.pojo.Edge>(graphHeadFactory)));


    vertices = vertexGraphKeys
      .groupBy("f0.id")
      .reduceGroup(new SetElementGraphIds<org.gradoop.common.model.impl.pojo.Vertex>())
      .withBroadcastSet(graphHeads, SetElementGraphIds.BROADCAST_GRAPHHEADS);

    edges = edgeGraphKeys
      .groupBy("f0.id")
      .reduceGroup(new SetElementGraphIds<org.gradoop.common.model.impl.pojo.Edge>())
      .withBroadcastSet(graphHeads, SetElementGraphIds.BROADCAST_GRAPHHEADS);



    return GraphCollection.fromDataSets(graphHeads, vertices, edges, getConfig());
  }

  @Override
  public GraphTransactions getGraphTransactions() throws IOException {
    return null;
  }
}
