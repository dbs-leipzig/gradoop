package org.gradoop.flink.io.impl.rdbms;

import java.io.IOException;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.junit.Test;

public class RDBMSDataSourceTest extends GradoopFlinkTestBase{
	
	@Test
	public void testRead() throws IOException{
		 String gdlPath = RDBMSDataSource.class
			      .getResource("/data/gdl/cycleTest.gdl")
			      .getFile();
		 
		 LogicalGraph expected = getLoaderFromFile(gdlPath)
				 .getLogicalGraphByVariable("expected");
	}
	
	public static void main(String[] args) throws IOException{
		 RDBMSDataSourceTest test = new RDBMSDataSourceTest();
		 test.testRead();
	}
}
