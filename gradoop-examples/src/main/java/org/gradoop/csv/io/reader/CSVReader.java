package org.gradoop.csv.io.reader;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
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
  private static final Pattern LINE_TOKEN_SEPARATOR = Pattern.compile("|");
  private Configuration conf;
  private boolean once = true;
  private boolean edge = false;
  private List<String> labels;
  private String[] types;
  private String[] properties;
  private List<Vertex> vList;

  private String[] getTokens(String line) {
    return LINE_TOKEN_SEPARATOR.split(line);
  }

  private void initialize(String firstLine) {
    String metaData = conf.get("Meta");
    this.labels.add(conf.get("Label"));
    try {
      BufferedReader in = new BufferedReader(new FileReader(metaData));
      String line;
      while ((line = in.readLine()) != null) {
        this.types = getTokens(line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    readFirstLine(firstLine);
    checkProperties();
  }

  private void checkProperties() {
    int ids = 0;
    for (int j = 0; j < properties.length - 2; j++) {
      if (properties[j].contains("id")) {
        ids++;
      }
    }
    if (ids > 1) {
      edge = true;
    }
  }

  private void readFirstLine(String line) {
    vList = Lists.newArrayList();
    once = false;
    this.properties = getTokens(line);
  }

  private Vertex readRemainingLines(String line) {
    String[] tokens = getTokens(line);
    long id = Long.parseLong(tokens[0]);
    Vertex vex = VertexFactory.createDefaultVertexWithLabels(id, labels, null);
    if (edge) {
    } else {
      for (int i = 1; i < properties.length - 2; i++) {
        switch (types[i]) {
        case "long":
          vex.addProperty(properties[i], Long.parseLong(tokens[i]));
          break;
        case "string":
          vex.addProperty(properties[i], tokens[i]);
          break;
        default:
          vex.addProperty(properties[i], tokens[i]);
          break;
        }
      }
    }
    return vex;
  }

  @Override
  public Vertex readVertex(String line) {
    return null;
  }

  @Override
  public List<Vertex> readVertexList(String line) {
    initialize(line);
    if (!once) {
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
