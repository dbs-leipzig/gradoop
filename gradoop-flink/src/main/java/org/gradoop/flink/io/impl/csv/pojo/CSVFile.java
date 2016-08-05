package org.gradoop.flink.io.impl.csv.pojo;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Created by stephan on 05.08.16.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name="csvfile")
//@XmlType(factoryClass=ObjectFactory.class,
//  factoryMethod="createCSVFile")
public class CSVFile {

  @XmlAttribute
  private String name;


  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return "csv: " + name;
  }
}
