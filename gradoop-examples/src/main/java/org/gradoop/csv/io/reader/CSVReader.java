package org.gradoop.csv.io.reader;

import org.gradoop.io.reader.VertexLineReader;
import org.gradoop.model.Vertex;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Reads csv input data
 */
public class CSVReader implements VertexLineReader {

  private static final Pattern LINE_TOKEN_SEPARATOR = Pattern.compile("|");

  private String path;

  private String[] getTokens(String line) {
    return LINE_TOKEN_SEPARATOR.split(line);
  }

  public CSVReader (String path){
    this.path=path;
    //initialize();
  }

  @Override
  public Vertex readVertex(String line) {
    return null;
  }

  @Override
  public List<Vertex> readVertexList(String line) {
    return null;
  }

  @Override
  public boolean supportsVertexLists() {
    return false;
  }
}
