package org.gradoop.flink.io.impl.csv.parser;

import org.gradoop.flink.io.impl.csv.pojo.CSVFile;
import org.gradoop.flink.io.impl.csv.pojo.DataSource;

import javax.xml.bind.annotation.XmlRegistry;

/**
 * Created by stephan on 05.08.16.
 */
@XmlRegistry
public class ObjectFactory {


  public static DataSource createDataSource() {
    return new DataSource();
  }


  public static CSVFile createCSVFile() {
    CSVFile file = new CSVFile();
//    file.setName("");
    return file;
  }

}
