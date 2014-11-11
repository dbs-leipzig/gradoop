package org.biiig.core.io;

import junit.framework.TestCase;

import java.io.*;
import java.util.Random;

public class AdjacencyListReaderTest extends TestCase {
  private static String TMP_DIR = System.getProperty("java.io.tmpdir");

  private String getCallingMethod() {
    return Thread.currentThread().getStackTrace()[1].getMethodName();
  }

  private String getTempFile() {
    return TMP_DIR + File.separator + getCallingMethod() + "_" + new Random().nextLong();
  }

  private BufferedReader createTestStream(String[] graph) throws IOException {
    String tmpFile = getTempFile();
    BufferedWriter bw = new BufferedWriter(new FileWriter(tmpFile));

    for (String line : graph) {
      bw.write(line);
      bw.newLine();
    }
    bw.flush();
    bw.close();

    return new BufferedReader(new FileReader(tmpFile));
  }
}