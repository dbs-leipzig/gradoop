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
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.functions.*;
import org.gradoop.flink.io.impl.csv.parser.XmlMetaParser;
import org.gradoop.flink.io.impl.csv.pojos.CsvExtension;
import org.gradoop.flink.io.impl.csv.pojos.Datasource;
import org.gradoop.flink.io.impl.csv.pojos.Domain;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Creates an EPGM instance from CSV files. Their format has to be defined
 * with a xml file. The schema for the xml is located at
 * 'resources/dara/csv/csv_format.xsd'.
 */
public class CSVDataSource extends CSVBase implements DataSource {
  /**
   * EPGM graph head factory
   */
  private GraphHeadFactory graphHeadFactory;
  /**
   * EPGM vertex factory
   */
  private VertexFactory vertexFactory;
  /**
   * EPGM edge factory
   */
  private EdgeFactory edgeFactory;

  /**
   * Creates a new data source. Paths can be local (file://) or HDFS (hdfs://).
   *
   * @param metaXmlPath xml file
   * @param csvDir csv directory
   * @param config Gradoop Flink configuration
   */
  public CSVDataSource(String metaXmlPath, String csvDir,
    GradoopFlinkConfig config) {
    super(metaXmlPath, csvDir, config);

    graphHeadFactory = config.getGraphHeadFactory();
    vertexFactory = config.getVertexFactory();
    edgeFactory = config.getEdgeFactory();
  }

  @Override
  public LogicalGraph getLogicalGraph() throws IOException {
    return getGraphCollection().reduce(new ReduceCombination());
  }

  @Override
  public GraphCollection getGraphCollection() throws IOException {
    ExecutionEnvironment env = getConfig().getExecutionEnvironment();
    DataSet<org.gradoop.common.model.impl.pojo.GraphHead> graphHeads;
    DataSet<org.gradoop.common.model.impl.pojo.Vertex> vertices;
    DataSet<org.gradoop.common.model.impl.pojo.Edge> edges;

    // parse the xml file to a datasource and select each csv object
    Datasource datasource = null;
    try {
      datasource = XmlMetaParser.parse(getXsdPath(), getMetaXmlPath());
    } catch (SAXException | JAXBException e) {
      e.printStackTrace();
    }
    List<CsvExtension> csvList = Lists.newArrayList();
    for (Domain domain : datasource.getDomain()) {
      for (CsvExtension csv : domain.getCsv()) {
        csv.setDatasourceName(datasource.getName());
        csv.setDomainName(domain.getName());
        csvList.add(csv);
      }
    }

    // load the content for each csv file described in the xml file
    CsvExtension first = csvList.remove(0);
    DataSet<Tuple2<CsvExtension, List<String>>> csvContent = env
      .readTextFile(getCsvDir() + first.getName())
      .reduceGroup(new CSVToContent(first));
    for (CsvExtension csvFile : csvList) {
      csvContent = csvContent
        .union(env
          .readTextFile(getCsvDir() + csvFile.getName())
          .reduceGroup(new CSVToContent(csvFile)));
    }

    //map each content line to an epgm element
    DataSet<EPGMElement> elements = csvContent
      .flatMap(new CSVToElement(graphHeadFactory, vertexFactory, edgeFactory));

    //get all the graph heads
    graphHeads = elements
      .filter(new CSVTypeFilter(GraphHead.class))
      .map(new EPGMElementToPojo<GraphHead>())
      .returns(graphHeadFactory.getType());

    //get all vertices
    vertices = elements
      .filter(
        new CSVTypeFilter(org.gradoop.common.model.impl.pojo.Vertex.class))
      .map(new EPGMElementToPojo<org.gradoop.common.model.impl.pojo.Vertex>())
      .returns(vertexFactory.getType());

    //create map from class key to gradoop id
    DataSet<Map<String, GradoopId>> vertexIds = vertices
      .map(new VertexToVertexIds())
      .reduceGroup(new VertexIdsToMap());

    try {
      vertexIds.print();
    } catch (Exception e) {
      e.printStackTrace();
    }

    try {
      System.out.println("vertexIds.count() = " + vertexIds.count());
    } catch (Exception e) {
      e.printStackTrace();
    }

    //get all edges and adjust the vertex keys to their corresponding gradoop id
    edges = elements
      .filter(new CSVTypeFilter(org.gradoop.common.model.impl.pojo.Edge.class))
      .map(new EPGMElementToPojo<org.gradoop.common.model.impl.pojo.Edge>())
      .returns(edgeFactory.getType())
      .map(new GradoopEdgeIds())
      .withBroadcastSet(vertexIds, CSVConstants.BROADCAST_ID_MAP);

    //get all graph keys from vertex properties
    DataSet<
      Tuple2<org.gradoop.common.model.impl.pojo.Vertex, String>> vertexGraphKeys
      = vertices
        .flatMap(
          new ElementToElementGraphKey<
            org.gradoop.common.model.impl.pojo.Vertex>());

    //get all graph keys from edge properties
    DataSet<Tuple2<org.gradoop.common.model.impl.pojo.Edge, String>>
      edgeGraphKeys = edges
      .flatMap(new ElementToElementGraphKey<org.gradoop.common.model.impl
        .pojo.Edge>());

    //create graph heads for vertices without any
    graphHeads = graphHeads
      .union(vertexGraphKeys
        .groupBy(1)
        .reduceGroup(
          new ElementGraphKeyToGraphHead<
            org.gradoop.common.model.impl.pojo.Vertex>(graphHeadFactory)))
      .union(edgeGraphKeys
          .groupBy(1)
          .reduceGroup(
            new ElementGraphKeyToGraphHead<
            org.gradoop.common.model.impl.pojo.Edge>(graphHeadFactory)));

    //set all graph heads
    vertices = vertexGraphKeys
      .groupBy("f0.id")
      .reduceGroup(
        new SetElementGraphIds<org.gradoop.common.model.impl.pojo.Vertex>())
      .withBroadcastSet(graphHeads, CSVConstants.BROADCAST_GRAPHHEADS);

    //set all graph heads
    edges = edgeGraphKeys
      .groupBy("f0.id")
      .reduceGroup(
        new SetElementGraphIds<org.gradoop.common.model.impl.pojo.Edge>())
      .withBroadcastSet(graphHeads, CSVConstants.BROADCAST_GRAPHHEADS);

    return GraphCollection.fromDataSets(
      graphHeads, vertices, edges, getConfig());
  }

  @Override
  public GraphTransactions getGraphTransactions() throws IOException {
    return getGraphCollection().toTransactions();
  }
}
