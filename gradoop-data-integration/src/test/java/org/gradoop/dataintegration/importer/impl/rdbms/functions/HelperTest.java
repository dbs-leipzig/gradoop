/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.dataintegration.importer.impl.rdbms.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.gradoop.dataintegration.importer.impl.rdbms.constants.RdbmsConstants;
import org.gradoop.dataintegration.importer.impl.rdbms.tuples.FkTuple;
import org.gradoop.dataintegration.importer.impl.rdbms.tuples.NameTypeTuple;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.MockitoAnnotations;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import static org.gradoop.dataintegration.importer.impl.rdbms.constants.RdbmsConstants.RdbmsType.MYSQL_TYPE;
import static org.gradoop.dataintegration.importer.impl.rdbms.constants.RdbmsConstants.RdbmsType.SQLSERVER_TYPE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class HelperTest extends GradoopFlinkTestBase {

  @Parameterized.Parameter
  public RdbmsConstants.RdbmsType rdbmsType;

  @Parameterized.Parameter(1)
  public int rowCount;

  @Parameterized.Parameter(2)
  public int row00;

  @Parameterized.Parameter(3)
  public int row01;

  @Parameterized.Parameter(4)
  public int row20;

  @Parameterized.Parameter(5)
  public int row21;

  @Parameterized.Parameter(6)
  public int row30;

  @Parameterized.Parameter(7)
  public int row31;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][]{{MYSQL_TYPE, 11, 2, 0, 2, 4, 5, 6}, {
      SQLSERVER_TYPE, 15, 0, 3, 6, 3, 9, 6}};
    return Arrays.asList(data);
  }


  @Test
  public void choosePartitionParametersTest() {
    getExecutionEnvironment().setParallelism(4);
    int parallelism = getExecutionEnvironment().getParallelism();

    assertArrayEquals(new Integer[]{row00, row01},
      Helper.choosePartitionParameters(rdbmsType, parallelism, rowCount)[0]);
    assertArrayEquals(new Integer[]{row20, row21},
      Helper.choosePartitionParameters(rdbmsType, parallelism, rowCount)[2]);
    assertArrayEquals(new Integer[]{row30, row31},
      Helper.choosePartitionParameters(rdbmsType, parallelism, rowCount)[3]);
  }

  @Test
  public void getTypeInfoTest() {
    assertEquals(BasicTypeInfo.STRING_TYPE_INFO,
      Helper.getTypeInfo(JDBCType.LONGVARCHAR, SQLSERVER_TYPE));
    assertEquals(BasicTypeInfo.INT_TYPE_INFO, Helper.getTypeInfo(JDBCType.SMALLINT, MYSQL_TYPE));
    assertEquals(BasicTypeInfo.BYTE_TYPE_INFO,
      Helper.getTypeInfo(JDBCType.VARBINARY, MYSQL_TYPE));
  }

  @Test
  public void getTableToVerticesQueryTest() {
    ArrayList<NameTypeTuple> primaryKeys = Lists.newArrayList();
    ArrayList<FkTuple> foreignKeys = Lists.newArrayList();
    ArrayList<NameTypeTuple> furtherAttributes = Lists.newArrayList();

    primaryKeys.add(new NameTypeTuple("pers_no", JDBCType.INTEGER));
    foreignKeys.add(new FkTuple("dept_no", JDBCType.INTEGER, "department_number", "department"));
    furtherAttributes.add(new NameTypeTuple("date_of_birth", JDBCType.DATE));

    assertEquals("SELECT pers_no,dept_no,date_of_birth FROM employees", Helper
      .getTableToVerticesQuery("employees", primaryKeys, foreignKeys, furtherAttributes,
        MYSQL_TYPE));

    assertEquals("SELECT [pers_no],[dept_no],[date_of_birth] FROM employees", Helper
      .getTableToVerticesQuery("employees", primaryKeys, foreignKeys, furtherAttributes,
        SQLSERVER_TYPE));
  }

  @Test
  public void getTableToEdgesQueryTest() {
    ArrayList<NameTypeTuple> furtherAttributes = Lists.newArrayList();

    furtherAttributes.add(new NameTypeTuple("since", JDBCType.DATE));

    assertEquals("SELECT pers_no,dept_no,since FROM dept_manager", Helper
      .getTableToEdgesQuery("dept_manager", "pers_no", "dept_no", furtherAttributes,
        MYSQL_TYPE));

    assertEquals("SELECT [pers_no],[dept_no],[since] FROM dept_manager", Helper
      .getTableToEdgesQuery("dept_manager", "pers_no", "dept_no", furtherAttributes,
        SQLSERVER_TYPE));
  }
}
