package org.gradoop.rdf.algorithms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.log4j.Logger;
import org.gradoop.GConstants;
import org.gradoop.utils.RDFPropertyXMLHandler;
import org.gradoop.model.Vertex;
import org.gradoop.storage.hbase.VertexHandler;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;

/**
 * RDF Instance Enrichment Map Reduce algorithm for vertices which are already
 * loaded to HBase.
 */
public class RDFInstanceEnrichment {
  /**
   * Logger
   */
  private static Logger LOG = Logger.getLogger(RDFInstanceEnrichment
    .class);
  /**
   * Some vertices will get no property at all, need dummy property
   */
  private static final String EMPTY_PROPERTY = "empty_property";
  /**
   * Dummy value
   */
  private static final String EMPTY_PROPERTY_VALUE = "";
  /**
   * EnrichMapper to read properties from the URL and write them to vertex
   * properties.
   */
  public static class EnrichMapper extends TableMapper<ImmutableBytesWritable,
    Put> {
    /**
     * vertex handler
     */
    private VertexHandler vertexHandler;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void setup(Context context) throws IOException,
      InterruptedException {
      LOG.info("=== start setup");
      Configuration conf = context.getConfiguration();

      Class<? extends VertexHandler> handlerClass = conf
        .getClass(GConstants.VERTEX_HANDLER_CLASS,
          GConstants.DEFAULT_VERTEX_HANDLER, VertexHandler.class);
      try {
        this.vertexHandler = handlerClass.getConstructor().newInstance();
      } catch (InvocationTargetException | NoSuchMethodException |
        InstantiationException | IllegalAccessException e) {
        e.printStackTrace();
      }
      LOG.info("=== end setup");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void map(ImmutableBytesWritable key, Result value,
      Context context) throws IOException, InterruptedException {
      LOG.info("=== map");

      Vertex v = vertexHandler.readVertex(value);
      Put put = new Put(vertexHandler.getRowKey(v.getID()));
      String url = v.getLabels().iterator().next();

      RDFPropertyXMLHandler handler = new RDFPropertyXMLHandler();
      try {
        HashSet<String[]> properties = handler.getLabelsForURI(url);
        if (!properties.isEmpty()) {
          for (String[] property : properties) {
            String k = property[0];
            String s = property[1];
            if (!s.isEmpty() || !s.equals("")) {
              v.addProperty(k, s);
            }
          }
          put = vertexHandler.writeProperties(put, v);
        } else {
          v.addProperty(EMPTY_PROPERTY, EMPTY_PROPERTY_VALUE);
          put = vertexHandler.writeProperties(put, v);
        }
      } catch (ParserConfigurationException | SAXException e) {
        e.printStackTrace();
        // too much queries -> parser exception, add dummy property
        v.addProperty(EMPTY_PROPERTY, EMPTY_PROPERTY_VALUE);
        put = vertexHandler.writeProperties(put, v);
      }
      context.write(key, put);
    }
  }
}
