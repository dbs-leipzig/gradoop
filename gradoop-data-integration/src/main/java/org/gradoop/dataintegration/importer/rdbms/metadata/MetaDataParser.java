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
package org.gradoop.dataintegration.importer.rdbms.metadata;

import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.gradoop.dataintegration.importer.rdbms.connection.Helper;
import org.gradoop.dataintegration.importer.rdbms.constants.RdbmsConstants.RdbmsType;
import org.gradoop.dataintegration.importer.rdbms.tuples.FkTuple;
import org.gradoop.dataintegration.importer.rdbms.tuples.NameTypeTuple;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Relational database schema.
 */
public class MetaDataParser {

  /**
   * Database connection.
   */
  private Connection connection;

  /**
   * Management type of connected rdbms.
   */
  private RdbmsType rdbmsType;

  /**
   * Relational database metadata.
   */
  private DatabaseMetaData metadata;

  /**
   * Parsed relational database tables.
   */
  private ArrayList<RdbmsTableBase> tableBase;

  /**
   * Parses the schema of a relational database and provides base classes for graph conversation.
   *
   * @param connection database connection
   * @param rdbmsType management system type of connected rdbms
   * @throws SQLException if connection is closed or invalid
   */
  public MetaDataParser(Connection connection, RdbmsType rdbmsType) throws SQLException {
    this.connection = connection;
    this.rdbmsType = rdbmsType;
    this.metadata = connection.getMetaData();
    this.tableBase = Lists.newArrayList();
  }

  /**
   * Parses the schema of the connected relational database to a metadata representation.
   *
   * @throws SQLException if database information are invalid or not accessible or connection is
   *                      broken
   */
  public void parse() throws SQLException {
    ResultSet rsTables = metadata.getTables(null, "%", "%", new String[]{"TABLE"});

    while (rsTables.next()) {
      String tableName = rsTables.getString("TABLE_NAME");
      String schemName = rsTables.getString("TABLE_SCHEM");

      // used to store primary key metadata representation
      ArrayList<NameTypeTuple> primaryKeys = Lists.newArrayList();

      // used to store foreign key metadata representation
      ArrayList<FkTuple> foreignKeys = Lists.newArrayList();

      // used to store further attributes metadata representation
      ArrayList<NameTypeTuple> furtherAttributes = Lists.newArrayList();

      // used to find further attributes, respectively no primary or
      // foreign key attributes
      ArrayList<String> pkfkAttributes = Lists.newArrayList();

      ResultSet rsPrimaryKeys = metadata.getPrimaryKeys(null, schemName, tableName);

      // parses primary keys if exists
      if (rsPrimaryKeys != null) {

        // assigning primary key name
        while (rsPrimaryKeys.next()) {
          primaryKeys.add(new NameTypeTuple(rsPrimaryKeys.getString("COLUMN_NAME"), null));
          pkfkAttributes.add(rsPrimaryKeys.getString("COLUMN_NAME"));
        }

        // assigning primary key data type
        for (NameTypeTuple pk : primaryKeys) {
          ResultSet rsColumns = metadata.getColumns(null, null, tableName, pk.f0);
          rsColumns.next();
          pk.f1 = JDBCType.valueOf(rsColumns.getInt("DATA_TYPE"));
        }
      }

      ResultSet rsForeignKeys = metadata.getImportedKeys(null, schemName, tableName);

      // parses foreign keys if exists
      if (rsForeignKeys != null) {

        // assigning foreign key name and name of belonging primary
        // and foreign key table
        while (rsForeignKeys.next()) {

          String refdTableName = rsForeignKeys.getString("PKTABLE_NAME");
          String refdTableSchem = rsForeignKeys.getString("PKTABLE_SCHEM");

          if (refdTableSchem == null) {
            foreignKeys.add(new FkTuple(rsForeignKeys.getString("FKCOLUMN_NAME"), null,
              rsForeignKeys.getString("PKCOLUMN_NAME"), refdTableName));
          } else {
            foreignKeys.add(new FkTuple(rsForeignKeys.getString("FKCOLUMN_NAME"), null,
              rsForeignKeys.getString("PKCOLUMN_NAME"), refdTableSchem + "." + refdTableName));
          }

          pkfkAttributes.add(rsForeignKeys.getString("FKCOLUMN_NAME"));
        }

        // assigning foreign key data type
        for (FkTuple fk : foreignKeys) {
          ResultSet rsColumns = metadata.getColumns(null, null, tableName, fk.f0);
          rsColumns.next();
          fk.f1 = JDBCType.valueOf(rsColumns.getInt("DATA_TYPE"));
        }
      }

      ResultSet rsAttributes = metadata.getColumns(null, schemName, tableName, null);

      // parses further attributes if exists
      // assigning attribute name and belonging data type
      while (rsAttributes.next()) {

        // catches unsupported data types
        if (!pkfkAttributes.contains(rsAttributes.getString("COLUMN_NAME")) &&
          JDBCType.valueOf(rsAttributes.getInt("DATA_TYPE")) != JDBCType.OTHER &&
          JDBCType.valueOf(rsAttributes.getInt("DATA_TYPE")) != JDBCType.ARRAY) {

          NameTypeTuple att = new NameTypeTuple(rsAttributes.getString("COLUMN_NAME"),
            JDBCType.valueOf(rsAttributes.getInt("DATA_TYPE")));

          furtherAttributes.add(att);
        }
      }

      // number of rows (needed for distributed data querying via
      // flink)
      int rowCount;
      if (schemName == null) {

        rowCount = Helper.getTableRowCount(connection, tableName);
        tableBase.add(
          new RdbmsTableBase(tableName, primaryKeys, foreignKeys, furtherAttributes, rowCount));
      } else {

        rowCount = Helper.getTableRowCount(connection, schemName + "." + tableName);
        tableBase.add(new RdbmsTableBase(schemName + "." + tableName, primaryKeys, foreignKeys,
          furtherAttributes, rowCount));
      }
    }
    connection.close();
  }

  /**
   * Creates metadata representations of tables going to convert to vertices.
   *
   * @return list containing {@link TableToVertex} instances.
   */
  public ArrayList<TableToVertex> getTablesToVertices() {
    ArrayList<TableToVertex> tablesToVertices = Lists.newArrayList();

    for (RdbmsTableBase tables : tableBase) {
      if (!(tables.getForeignKeys().size() == 2) || !(tables.getPrimaryKeys().size() == 2)) {

        tablesToVertices
          .add(new TableToVertex(rdbmsType, tables.getTableName(), tables.getPrimaryKeys(),
            tables.getForeignKeys(), tables.getFurtherAttributes(), tables.getRowCount()));
      }
    }
    return tablesToVertices;
  }

  /**
   * Creates metadata representations of tables going to convert to edges.
   *
   * @return list containing {@link TableToEdge} instances.
   */
  public ArrayList<TableToEdge> getTablesToEdges() {
    ArrayList<TableToEdge> tablesToEdges = Lists.newArrayList();

    for (RdbmsTableBase table : tableBase) {
      if (table.getForeignKeys() != null) {

        String tableName = table.getTableName();
        int rowCount = table.getRowCount();

        // table tuples going to convert to edges
        if (table.getForeignKeys().size() == 2 && table.getPrimaryKeys().size() == 2) {

          String referencedTableNameOne = table.getForeignKeys().get(0).f3;
          String referencedTableNameTwo = table.getForeignKeys().get(1).f3;

          NameTypeTuple foreignKeyOne = new NameTypeTuple(table.getForeignKeys().get(0).f0,
            table.getForeignKeys().get(0).f1);
          NameTypeTuple foreignKeyTwo = new NameTypeTuple(table.getForeignKeys().get(1).f0,
            table.getForeignKeys().get(1).f1);

          tablesToEdges.add(
            new TableToEdge(tableName, rdbmsType, table.getFurtherAttributes(),
              rowCount, referencedTableNameOne, referencedTableNameTwo, foreignKeyOne,
              foreignKeyTwo, false));
        } else {

          for (FkTuple foreignKey : table.getForeignKeys()) {

            String referencedTableName = foreignKey.getReferencedTablename();

            NameTypeTuple startAttribute = new NameTypeTuple(foreignKey.f0, foreignKey.f1);
            NameTypeTuple endAttribute = new NameTypeTuple(foreignKey.f2, null);

            tablesToEdges.add(
              new TableToEdge(tableName, rdbmsType, null,
                0, null, referencedTableName, startAttribute,
                endAttribute, true));
          }
        }
      }
    }
    return tablesToEdges;
  }

  public DatabaseMetaData getMetadata() {
    return metadata;
  }

  public void setMetadata(DatabaseMetaData metadata) {
    this.metadata = metadata;
  }

  public ArrayList<RdbmsTableBase> getTableBase() {
    return tableBase;
  }
}
