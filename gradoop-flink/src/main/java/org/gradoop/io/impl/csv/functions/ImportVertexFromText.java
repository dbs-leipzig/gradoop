package org.gradoop.io.impl.csv.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.io.Text;
import org.gradoop.io.impl.csv.CSVDataSource;
import org.gradoop.io.impl.graph.tuples.ImportVertex;
import org.gradoop.model.impl.properties.PropertyList;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class ImportVertexFromText extends RichMapFunction<String,
  ImportVertex<String>> {

  private String delimiter;

  public ImportVertexFromText(String delimiter) {
    this.delimiter = delimiter;
  }

  @Override
  public ImportVertex<String> map(String line) throws Exception {
    String[] fields = line.split(delimiter);
    PropertyList properties = new PropertyList();
    String id = "";
    String label = "";

    return new ImportVertex<String>(id, label, properties);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    File file = getRuntimeContext().getDistributedCache().getFile(
      CSVDataSource.CACHED_FILE);
    BufferedReader reader = new BufferedReader(new FileReader(file));
    String tempString;
    while ((tempString = reader.readLine()) != null) {

    }

    reader.close();
  }
}
