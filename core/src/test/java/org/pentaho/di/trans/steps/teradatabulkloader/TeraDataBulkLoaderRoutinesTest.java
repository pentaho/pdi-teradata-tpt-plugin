/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2020 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.pentaho.di.trans.steps.teradatabulkloader;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.variables.Variables;

import java.io.File;
import java.util.UUID;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;
import static org.mockito.internal.util.reflection.Whitebox.setInternalState;

public class TeraDataBulkLoaderRoutinesTest {

  private TeraDataBulkLoaderRoutines teraDataBulkLoaderRoutines;
  private TeraDataBulkLoader teraDataBulkLoaderMock;
  private TeraDataBulkLoaderMeta teraDataBulkLoaderMetaMock;
  private DatabaseMeta dataBaseMetaDataMock;

  @Before
  public void setup() {
    teraDataBulkLoaderMock = mock( TeraDataBulkLoader.class );
    teraDataBulkLoaderMetaMock = mock( TeraDataBulkLoaderMeta.class );
    dataBaseMetaDataMock = mock( DatabaseMeta.class );
    teraDataBulkLoaderRoutines = new TeraDataBulkLoaderRoutines( teraDataBulkLoaderMock, teraDataBulkLoaderMetaMock );
  }

  @Test
  public void getTargetSchemaEmptyTest() {
    when( teraDataBulkLoaderMetaMock.getSchemaName() ).thenReturn( "" );
    when( teraDataBulkLoaderMetaMock.getDbName() ).thenReturn( "dbName" );
    assertEquals( "dbName", teraDataBulkLoaderRoutines.getTargetSchema( true ) );
  }

  @Test
  public void getTargetSchemaTest() {
    when( teraDataBulkLoaderMetaMock.getSchemaName() ).thenReturn( "schemaName" );
    when( teraDataBulkLoaderMetaMock.getDbName() ).thenReturn( "dbName" );
    assertEquals( "schemaName", teraDataBulkLoaderRoutines.getTargetSchema( true ) );
  }

  @Test
  public void getTargetSchemaNullTest() {
    when( teraDataBulkLoaderMetaMock.getSchemaName() ).thenReturn( null );
    when( teraDataBulkLoaderMetaMock.getDbName() ).thenReturn( "dbName" );
    assertEquals( "dbName", teraDataBulkLoaderRoutines.getTargetSchema( true ) );
  }

  @Test
  public void getDataConnectorTest() {
    when( teraDataBulkLoaderMetaMock.getSchemaName() ).thenReturn( "schemaName" );
    when( teraDataBulkLoaderMetaMock.getAccessLogFile() ).thenReturn( "accessLog" );
    when( teraDataBulkLoaderMetaMock.getFifoFileName() ).thenReturn( "fifoFileName" );
    setInternalState( teraDataBulkLoaderRoutines, "parent", null );
    String expectedDataConnector = "DEFINE OPERATOR ACCESS_MODULE_READER\n"
      + "TYPE DATACONNECTOR PRODUCER\n"
      + "SCHEMA schemaName\n"
      + "ATTRIBUTES\n"
      + "  (\n"
      + "     VARCHAR PrivateLogName = 'accessLog',\n"
      + "     VARCHAR AccessModuleName = 'np_axsmod.so',\n"
      + "     VARCHAR FileName = 'fifoFileName',\n"
      + "     VARCHAR Format = 'Unformatted',\n"
      + "     VARCHAR OpenMode = 'Read'\n"
      + "  );\n";
    assertEquals( expectedDataConnector, teraDataBulkLoaderRoutines.getDataConnector( true ).toString() );
  }

  @Test
  public void getDdlOptionsPreviewTest() {
    when( teraDataBulkLoaderMetaMock.getDatabaseMeta() ).thenReturn( dataBaseMetaDataMock );
    when( dataBaseMetaDataMock.getHostname() ).thenReturn( "hostname" );
    when( dataBaseMetaDataMock.getUsername() ).thenReturn( "username" );
    String password = "myPassword";
    String expectedDdlOptionsPreview = "DEFINE OPERATOR DDL_OPERATOR\n"
      + "TYPE DDL\n"
      + "ATTRIBUTES\n"
      + "  (\n"
      + "     VARCHAR TdpId              = 'hostname',\n"
      + "     VARCHAR UserName           = 'username',\n"
      + "     VARCHAR UserPassword       = 'myPassword',\n"
      + "     VARCHAR ErrorList          = '3807'\n"
      + "  );\n";
    assertEquals( expectedDdlOptionsPreview, teraDataBulkLoaderRoutines.getDdlOptions( true, password ).toString() );
  }

  @Test
  public void getDdlOptionsTest() {
    when( teraDataBulkLoaderMetaMock.getDatabaseMeta() ).thenReturn( dataBaseMetaDataMock );
    when( dataBaseMetaDataMock.getHostname() ).thenReturn( "hostname" );
    when( dataBaseMetaDataMock.getUsername() ).thenReturn( "username" );
    when( dataBaseMetaDataMock.getPassword() ).thenReturn( "passwordFromMeta" );
    setInternalState( teraDataBulkLoaderRoutines, "parent", teraDataBulkLoaderMock );
    String password = "myPassword";
    String expectedDdlOptionsPreview = "DEFINE OPERATOR DDL_OPERATOR\n"
      + "TYPE DDL\n"
      + "ATTRIBUTES\n"
      + "  (\n"
      + "     VARCHAR TdpId              = 'hostname',\n"
      + "     VARCHAR UserName           = 'username',\n"
      + "     VARCHAR UserPassword       = 'passwordFromMeta',\n"
      + "     VARCHAR ErrorList          = '3807'\n"
      + "  );\n";
    assertEquals( expectedDdlOptionsPreview, teraDataBulkLoaderRoutines.getDdlOptions( false, password ).toString() );
  }

  @Test
  public void getUpdateOptionsPreviewTest() {
    when( teraDataBulkLoaderMetaMock.getDatabaseMeta() ).thenReturn( dataBaseMetaDataMock );
    when( dataBaseMetaDataMock.getHostname() ).thenReturn( "hostname" );
    when( dataBaseMetaDataMock.getUsername() ).thenReturn( "username" );
    when( teraDataBulkLoaderMetaMock.getUpdateLogFile() ).thenReturn( "logFile" );
    when( teraDataBulkLoaderMetaMock.getLogTable() ).thenReturn( "logTable" );
    when( teraDataBulkLoaderMetaMock.getTableName() ).thenReturn( "tableName" );
    when( teraDataBulkLoaderMetaMock.getWorkTable() ).thenReturn( "workTable" );
    when( teraDataBulkLoaderMetaMock.getErrorTable() ).thenReturn( "errorTable" );
    when( teraDataBulkLoaderMetaMock.getErrorTable2() ).thenReturn( "errorTable2" );
    when( teraDataBulkLoaderMetaMock.getDbName() ).thenReturn( "dbName" );
    String password = "myPassword";
    String expectedUpdataOptionsPreview = "DEFINE OPERATOR UPDATE_OPERATOR\n"
      + "TYPE UPDATE\n"
      + "SCHEMA *\n"
      + "ATTRIBUTES\n"
      + "  (\n"
      + "     VARCHAR TdpId              = 'hostname',\n"
      + "     VARCHAR PrivateLogName     = 'logFile',\n"
      + "     VARCHAR UserName           = 'username',\n"
      + "     VARCHAR UserPassword       = 'myPassword',\n"
      + "     VARCHAR LogTable           = 'logTable',\n"
      + "     VARCHAR TargetTable        = 'dbName.tableName',\n"
      + "     VARCHAR WorkTable          = 'workTable',\n"
      + "     VARCHAR ErrorTable1        = 'errorTable',\n"
      + "     VARCHAR ErrorTable2        = 'errorTable2'\n"
      + "  );\n";
    assertEquals( expectedUpdataOptionsPreview, teraDataBulkLoaderRoutines.getUpdateOptions( true, password ).toString() );
  }

  @Test
  public void getUpdateOptionsTest() {
    when( teraDataBulkLoaderMetaMock.getDatabaseMeta() ).thenReturn( dataBaseMetaDataMock );
    when( dataBaseMetaDataMock.getHostname() ).thenReturn( "hostname" );
    when( dataBaseMetaDataMock.getUsername() ).thenReturn( "username" );
    when( dataBaseMetaDataMock.getPassword() ).thenReturn( "passwordFromMeta" );
    when( teraDataBulkLoaderMetaMock.getUpdateLogFile() ).thenReturn( "logFile" );
    when( teraDataBulkLoaderMetaMock.getLogTable() ).thenReturn( "logTable" );
    when( teraDataBulkLoaderMetaMock.getTableName() ).thenReturn( "tableName" );
    when( teraDataBulkLoaderMetaMock.getWorkTable() ).thenReturn( "workTable" );
    when( teraDataBulkLoaderMetaMock.getErrorTable() ).thenReturn( "errorTable" );
    when( teraDataBulkLoaderMetaMock.getErrorTable2() ).thenReturn( "errorTable2" );
    when( teraDataBulkLoaderMetaMock.getDbName() ).thenReturn( "dbName" );
    setInternalState( teraDataBulkLoaderRoutines, "parent", teraDataBulkLoaderMock );
    Variables variables = new Variables();
    //Init the variables in the step
    setInternalState( teraDataBulkLoaderMock, "variables", variables );
    when( teraDataBulkLoaderMock.environmentSubstitute( anyString() ) ).thenCallRealMethod();
    String password = "myPassword";
    String expectedUpdataOptionsPreview = "DEFINE OPERATOR UPDATE_OPERATOR\n"
      + "TYPE UPDATE\n"
      + "SCHEMA *\n"
      + "ATTRIBUTES\n"
      + "  (\n"
      + "     VARCHAR TdpId              = 'hostname',\n"
      + "     VARCHAR PrivateLogName     = 'logFile',\n"
      + "     VARCHAR UserName           = 'username',\n"
      + "     VARCHAR UserPassword       = 'passwordFromMeta',\n"
      + "     VARCHAR LogTable           = 'logTable',\n"
      + "     VARCHAR TargetTable        = 'dbName.tableName',\n"
      + "     VARCHAR WorkTable          = 'workTable',\n"
      + "     VARCHAR ErrorTable1        = 'errorTable',\n"
      + "     VARCHAR ErrorTable2        = 'errorTable2'\n"
      + "  );\n";
    assertEquals( expectedUpdataOptionsPreview, teraDataBulkLoaderRoutines.getUpdateOptions( false, password ).toString() );
  }

  @Test
  public void createScriptFileTest() {
    String expectedFile = "myFile";
    //Init the variables in the step
    setInternalState( teraDataBulkLoaderMock, "variables", new Variables() );
    //Return the filename, in the end the script should contain this filename.
    when( teraDataBulkLoaderMetaMock.getExistingScriptFile() ).thenReturn( expectedFile );
    when( teraDataBulkLoaderMock.environmentSubstitute( anyString() ) ).thenCallRealMethod();
    File file = new File( expectedFile );
    try {
      //Need to exist to simulate the test
      file.createNewFile();
      file.deleteOnExit();
      String script = teraDataBulkLoaderRoutines.createScriptFile();
      assertTrue( script.contains( expectedFile ) );
    } catch ( Exception ex ) {
      fail( ex.getMessage() );
    } finally {
      file.delete();
    }
  }

  @Test
  public void createScriptFileWithEnvVariablesTest() {
    String expectedFile = "file";
    String variable = "FileVariable";
    Variables variables = new Variables();
    variables.setVariable( variable, expectedFile );
    //Init the variables in the step
    setInternalState( teraDataBulkLoaderMock, "variables", variables );
    //Return the variable instead of the filename, in the end the script should contain filename and not the variable name.
    when( teraDataBulkLoaderMetaMock.getExistingScriptFile() ).thenReturn( "${" + variable + "}" );
    when( teraDataBulkLoaderMock.environmentSubstitute( anyString() ) ).thenCallRealMethod();
    File file = new File( expectedFile );
    try {
      //Need to exist to simulate the test
      file.createNewFile();
      file.deleteOnExit();
      String script = teraDataBulkLoaderRoutines.createScriptFile();
      assertTrue( script.contains( expectedFile ) );
    } catch ( Exception ex ) {
      fail( ex.getMessage() );
    } finally {
      file.delete();
    }
  }

  @Test
  public void getDropTablesTest() {
    when( teraDataBulkLoaderMetaMock.getDropErrorTable() ).thenReturn( true );
    when( teraDataBulkLoaderMetaMock.getDropErrorTable2() ).thenReturn( true );
    when( teraDataBulkLoaderMetaMock.getDropLogTable() ).thenReturn( true );
    when( teraDataBulkLoaderMetaMock.getDropWorkTable() ).thenReturn( true );
    when( teraDataBulkLoaderMetaMock.getLogTable() ).thenReturn( "logTable" );
    when( teraDataBulkLoaderMetaMock.getWorkTable() ).thenReturn( "workTable" );
    when( teraDataBulkLoaderMetaMock.getErrorTable() ).thenReturn( "errorTable" );
    when( teraDataBulkLoaderMetaMock.getErrorTable2() ).thenReturn( "errorTable2" );
    String expectedDropTables = "STEP Setup_tables(\n"
      + "APPLY \n"
      + "     ('DROP TABLE logTable'),\n"
      + "     ('DROP TABLE workTable'),\n"
      + "     ('DROP TABLE errorTable'),\n"
      + "     ('DROP TABLE errorTable2')\n"
      + "\n"
      + "TO OPERATOR ( DDL_OPERATOR )\n"
      + ";\n"
      + "\n"
      + ");\n";
    String dropTables = teraDataBulkLoaderRoutines.getDropTables( true );
    assertEquals( expectedDropTables, dropTables );
  }

  @Test
  public void testCreateUpsertCommandPreview() {

    String tableName = UUID.randomUUID().toString();
    String schemaName = UUID.randomUUID().toString();

    String[] fieldTable = new String[] {};
    String[] fieldStream = new String[] {};
    Boolean[] fieldUpdate = new Boolean[] {};

    when( teraDataBulkLoaderMetaMock.getSchemaName() ).thenReturn( schemaName );
    when( teraDataBulkLoaderMetaMock.getTableName() ).thenReturn( tableName );
    when( teraDataBulkLoaderMetaMock.getFieldTable() ).thenReturn( fieldTable );
    when( teraDataBulkLoaderMetaMock.getFieldStream() ).thenReturn( fieldStream );
    when( teraDataBulkLoaderMetaMock.getFieldUpdate() ).thenReturn( fieldUpdate );
    when( teraDataBulkLoaderMetaMock.getKeyStream() ).thenReturn( new String[] {} );

    String upsertCommand = this.teraDataBulkLoaderRoutines.createUpsertCommand( true );
    assertEquals( true, upsertCommand.startsWith( "' UPDATE " + schemaName + "." + tableName + " SET\n"  ) );

  }
}
