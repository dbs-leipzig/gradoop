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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.rdf.examples;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.log4j.Logger;
import org.gradoop.GConstants;
import org.gradoop.model.Vertex;
import org.gradoop.storage.GraphStore;
import org.gradoop.utils.ConfigurationUtils;
import org.gradoop.storage.hbase.EPGGraphHandler;
import org.gradoop.storage.hbase.EPGVertexHandler;
import org.gradoop.storage.hbase.HBaseGraphStoreFactory;
import org.gradoop.utils.RDFPropertyXMLHandler;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.HashSet;
import java.util.Iterator;

/**
 * RDF Instance Enrichment Driver
 */
public class RDFInstanceEnrichmentDriver extends Configured implements Tool {
  /**
   * Logger
   */
  private static Logger LOG = Logger.getLogger(RDFInstanceEnrichmentDriver
    .class);
  /**
   * labels zu be checked
   */
  private static final String[] LABELS = new String[]{"rdfs:label",
                                                      "skos:prefLabel",
                                                      "gn:name"};
  /**
   * Some vertices will get no property at all, need dummy property
   */
  private static final String EMPTY_PROPERTY = "empty_property";
  /**
   * Some vertices will get no property at all, need dummy property
   */
  private static final String NO_PROPERTY = "no_property";
  /**
   * Dummy value
   */
  private static final String EMPTY_PROPERTY_VALUE = "";

  static {
    Configuration.addDefaultResource("hbase-site.xml");
  }

  /**
   * Starting point for RDF analysis pipeline.
   *
   * @param args driver arguments
   * @return Exit code (0 - ok)
   * @throws Exception
   */
  @Override
  public int run(String[] args) throws Exception {

    Configuration conf = getConf();
    CommandLine cmd = LoadConfUtils.parseArgs(args);

    if (cmd == null) {
      return 0;
    }

    String tablePrefix = cmd.getOptionValue(ConfigurationUtils
      .OPTION_TABLE_PREFIX, "");
    String tableName = tablePrefix + GConstants.DEFAULT_TABLE_VERTICES;

    // Open HBase tables
    GraphStore graphStore = HBaseGraphStoreFactory.createOrOpenGraphStore(conf,
      new EPGVertexHandler(), new EPGGraphHandler(), tablePrefix);

    LOG.info("=== graphStore opened with tableName " + tableName);
    enrich(graphStore, tableName);

    return 0;
  }

  /**
   * Enrich graph store with additional information from URLs
   * @param graphStore graph store to be handled
   * @param tableName HBase table name
   * @throws Exception
   */
  public void enrich(GraphStore graphStore, String tableName) throws Exception {
    RDFPropertyXMLHandler handler = new RDFPropertyXMLHandler();
    Iterator<Vertex> vertices = graphStore.getVertices(tableName, 30);
    int error = 0;
    int complete = 0;
    int exist = 0;
    while (vertices.hasNext()) {
      Vertex vertex = vertices.next();
      try {
        if (!containsLabels(vertex)) {
          vertex = getProperties(handler, vertex);
        } else {
          ++exist;
          LOG.info("Existing count: " + exist);
          continue;
        }
      } catch (ParserConfigurationException | SAXException |
        ConnectTimeoutException | HttpHostConnectException |
        SocketTimeoutException e) {
        //e.printStackTrace();
        LOG.info(e.toString());
        // too much queries -> parser exception, add dummy property
        vertex.addProperty(EMPTY_PROPERTY, EMPTY_PROPERTY_VALUE);
        ++error;
        LOG.info("Error count: " + error);
      }
      graphStore.writeVertex(vertex);
      ++complete;
      LOG.info("Completed count: " + complete);
    }
    graphStore.close();
  }

  /**
   * Get additional properties for a vertex via http request
   * @param handler xml rdf property handler
   * @param vertex vertex
   * @return enriched vertex
   * @throws IOException
   * @throws ParserConfigurationException
   * @throws SAXException
   */
  private Vertex getProperties(RDFPropertyXMLHandler handler, Vertex vertex)
      throws IOException, ParserConfigurationException, SAXException {
    String url = vertex.getLabel();
    HashSet<String[]> properties = handler.getLabelsForURI(url);

    return enrichVertexWithProperties(vertex, properties);
  }

  /**
   * Write all extracted properties to the vertex object.
   * @param vertex vertex to be written to
   * @param properties properties from XML handler
   * @return enriched vertex
   */
  private Vertex enrichVertexWithProperties(Vertex vertex,
    HashSet<String[]> properties) {
    if (!properties.isEmpty()) {
      for (String[] property : properties) {
        String key = property[0];
        String value = property[1];
        if (!value.isEmpty() || !value.equals("")) {
          vertex.addProperty(key, value);
        }
      }
    } else {
      vertex.addProperty(NO_PROPERTY, EMPTY_PROPERTY_VALUE);
    }
    return vertex;
  }

  /**
   * Checks, if vertex already contains specific label property keys
   * @param vertex vertex
   * @return true, if a label is contained, false otherwise
   */
  private boolean containsLabels(Vertex vertex) {
    if (vertex.getPropertyCount() != 0) {
      LOG.info("propCount = " + vertex.getPropertyCount());
      for (String label : LABELS) {
        for (String s : vertex.getPropertyKeys()) {
          LOG.info("string: " + vertex.getProperty(s));
          if (s.contains(label)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  /**
   * Runs the job.
   *
   * @param args command line arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    System.exit(ToolRunner.run(conf, new RDFInstanceEnrichmentDriver(), args));
  }

  /**
   * Configuration params for
   * {@link org.gradoop.rdf.examples.RDFInstanceEnrichmentDriver}.
   */
  public static class LoadConfUtils extends ConfigurationUtils {
    /**
     * Parses the given arguments.
     *
     * @param args command line arguments
     * @return parsed command line
     * @throws org.apache.commons.cli.ParseException
     */
    public static CommandLine parseArgs(final String[] args) throws
      ParseException {

      CommandLineParser parser = new BasicParser();
      CommandLine cmd = parser.parse(OPTIONS, args);

      if (cmd.hasOption(OPTION_HELP)) {
        printHelp();
        return null;
      }

      return cmd;
    }
  }
}
