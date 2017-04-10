package org.gradoop.benchmark.nesting.serializers;


import org.apache.flink.api.common.io.RichOutputFormat;

import javax.security.auth.login.Configuration;
import java.io.IOException;

public  class Bogus<T> extends RichOutputFormat<T> {
  @Override
  public void configure(org.apache.flink.configuration.Configuration parameters) {
  }

  @Override
  public void open(int taskNumber, int numTasks) throws IOException {

  }

  @Override
  public void writeRecord(T record) throws IOException {

  }

  @Override
  public void close() throws IOException {

  }
}