package org.gradoop.csv.io.reader;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.gradoop.io.reader.ConfigurableVertexLineReader;
import org.gradoop.model.Vertex;
import org.gradoop.model.impl.VertexFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Reads csv input data
 */
public class CSVReader implements ConfigurableVertexLineReader {
  private Logger Log = Logger.getLogger(CSVReader.class);
  private static final Pattern LINE_TOKEN_SEPARATOR = Pattern.compile("\\|");
  private Configuration conf;
  private boolean once = true;
  private boolean node = false;
  private List<String> labels;
  private String[] types;
  private String[] properties;
  private List<Vertex> vList;

  public static final String META_DATA = "META_DATA";
  public static final String TYPE = "TYPE";
  public static final String LABEL = "LABEL";

  private String[] getTokens(String line) {
    return LINE_TOKEN_SEPARATOR.split(line);
  }

  private void initializeAndReadFirstLine(String line) {
    Log.info("###INITIALIZE");
    vList = Lists.newArrayList();
    labels = Lists.newArrayList();
    Log.info("###ReadFirstLine");
    this.properties = getTokens(line);
    for (int i = 0; i < properties.length; i++) {
      Log.info(properties[i]);
    }
    String metaData = conf.get(META_DATA);
    labels.add(conf.get(LABEL));
//    this.types =
//      new String[]{"long", "string", "string", "string", "string", "string",
//                   "string", "string",};


    try {
      BufferedReader in = new BufferedReader(new FileReader(metaData));
      String datLine;
      while ((datLine = in.readLine()) != null) {
        this.types = getTokens(datLine);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    checkType();
    once = false;
  }

  private void checkType() {
    Log.info("###checkType");
    if (conf.get(TYPE).equals("node")) {
      this.node = true;
    }
    Log.info(node);
  }

  private Vertex readRemainingLines(String line) {
    Log.info("###ReadRemainingLines");
    String[] tokens = getTokens(line);
    long id = Long.parseLong(tokens[0]);
    Log.info(id);
    Log.info(labels.get(0));
    Vertex vex = VertexFactory.createDefaultVertexWithLabels(id, labels, null);
    if (node) {
      for (int i = 1; i < properties.length; i++) {
        switch (types[i]) {
        case "long":
          vex.addProperty(properties[i], Long.parseLong(tokens[i]));
          Log.info(tokens[i]);
          break;
        case "string":
          vex.addProperty(properties[i], tokens[i]);
          Log.info(tokens[i]);
          break;
        default:
          vex.addProperty(properties[i], tokens[i]);
          break;
        }
      }
    } else {
      //If edge
    }
    return vex;
  }

  @Override
  public Vertex readVertex(String line) {
    return null;
  }

  @Override
  public List<Vertex> readVertexList(String line) {
    if (once) {
      initializeAndReadFirstLine(line);
    } else {
      vList.add(readRemainingLines(line));
    }
    return vList;
  }

  @Override
  public boolean supportsVertexLists() {
    return false;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
}
