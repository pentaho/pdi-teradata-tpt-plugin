/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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
import org.mockito.internal.util.reflection.Whitebox;
import org.pentaho.di.core.database.DatabaseMeta;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;

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
    assertEquals("dbName", teraDataBulkLoaderRoutines.getTargetSchema() );
  }

  @Test
  public void getTargetSchemaTest() {
    when( teraDataBulkLoaderMetaMock.getSchemaName() ).thenReturn( "schemaName" );
    when( teraDataBulkLoaderMetaMock.getDbName() ).thenReturn( "dbName" );
    assertEquals("schemaName", teraDataBulkLoaderRoutines.getTargetSchema() );
  }

  @Test
  public void getTargetSchemaNullTest() {
    when( teraDataBulkLoaderMetaMock.getSchemaName() ).thenReturn( null );
    when( teraDataBulkLoaderMetaMock.getDbName() ).thenReturn( "dbName" );
    assertEquals("dbName", teraDataBulkLoaderRoutines.getTargetSchema() );
  }

  @Test
  public void getDataConnectorTest() {
    when( teraDataBulkLoaderMetaMock.getSchemaName() ).thenReturn( "schemaName" );
    when( teraDataBulkLoaderMetaMock.getAccessLogFile() ).thenReturn( "accessLog" );
    when( teraDataBulkLoaderMetaMock.getFifoFileName() ).thenReturn( "fifoFileName" );
    Whitebox.setInternalState( teraDataBulkLoaderRoutines, "parent", null );
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
    assertEquals( expectedDataConnector, teraDataBulkLoaderRoutines.getDataConnector().toString() );
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
    when( dataBaseMetaDataMock.getPassword() ).thenReturn( "passwordFromMeta");
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
      + "     VARCHAR UserPassword       = 'passwordFromMeta',\n"
      + "     VARCHAR LogTable           = 'logTable',\n"
      + "     VARCHAR TargetTable        = 'dbName.tableName',\n"
      + "     VARCHAR WorkTable          = 'workTable',\n"
      + "     VARCHAR ErrorTable1        = 'errorTable',\n"
      + "     VARCHAR ErrorTable2        = 'errorTable2'\n"
      + "  );\n";
    assertEquals( expectedUpdataOptionsPreview, teraDataBulkLoaderRoutines.getUpdateOptions( false, password ).toString() );
  }
}
