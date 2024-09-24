/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/
package org.pentaho.di.trans.steps.teradatabulkloader;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.variables.Variables;

import java.io.File;
import java.lang.reflect.Field;
import java.util.UUID;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;
//import static org.mockito.internal.util.reflection.Whitebox.setInternalState;

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
  public void getDataConnectorTest() throws NoSuchFieldException, IllegalAccessException {
    when( teraDataBulkLoaderMetaMock.getSchemaName() ).thenReturn( "schemaName" );
    when( teraDataBulkLoaderMetaMock.getAccessLogFile() ).thenReturn( "accessLog" );
    when( teraDataBulkLoaderMetaMock.getFifoFileName() ).thenReturn( "fifoFileName" );
    //setInternalState( teraDataBulkLoaderRoutines, "parent", null );
    Field fieldParent= TeraDataBulkLoaderRoutines.class.getDeclaredField("parent");
    fieldParent.setAccessible(true);
    fieldParent.set( teraDataBulkLoaderRoutines, null );
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
  public void getDdlOptionsTest() throws NoSuchFieldException, IllegalAccessException {
    when( teraDataBulkLoaderMetaMock.getDatabaseMeta() ).thenReturn( dataBaseMetaDataMock );
    when( dataBaseMetaDataMock.getHostname() ).thenReturn( "hostname" );
    when( dataBaseMetaDataMock.getUsername() ).thenReturn( "username" );
    when( dataBaseMetaDataMock.getPassword() ).thenReturn( "passwordFromMeta" );
    //setInternalState( teraDataBulkLoaderRoutines, "parent", teraDataBulkLoaderMock );
    Field field= TeraDataBulkLoaderRoutines.class.getDeclaredField("parent");
    field.setAccessible(true);
    field.set( teraDataBulkLoaderRoutines, teraDataBulkLoaderMock );
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
  public void getUpdateOptionsTest() throws NoSuchFieldException, IllegalAccessException {
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
    //setInternalState( teraDataBulkLoaderRoutines, "parent", teraDataBulkLoaderMock );
    Field field= TeraDataBulkLoaderRoutines.class.getDeclaredField("parent");
    field.setAccessible(true);
    field.set( teraDataBulkLoaderRoutines, teraDataBulkLoaderMock );
    Variables variables = new Variables();
    //Init the variables in the step
    //setInternalState( teraDataBulkLoaderMock, "variables", variables );
    Field fieldVar= TeraDataBulkLoader.class.getSuperclass().getDeclaredField("variables");
    fieldVar.setAccessible(true);
    fieldVar.set( teraDataBulkLoaderMock, variables );
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
  public void createScriptFileTest() throws NoSuchFieldException, IllegalAccessException {
    String expectedFile = "myFile";
    //Init the variables in the step
    //setInternalState( teraDataBulkLoaderMock, "variables", new Variables() );
    Field field= TeraDataBulkLoader.class.getSuperclass().getDeclaredField("variables");
    field.setAccessible(true);
    field.set( teraDataBulkLoaderMock, new Variables() );
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
  public void createScriptFileWithEnvVariablesTest() throws NoSuchFieldException, IllegalAccessException {
    String expectedFile = "file";
    String variable = "FileVariable";
    Variables variables = new Variables();
    variables.setVariable( variable, expectedFile );
    //Init the variables in the step
    //setInternalState( teraDataBulkLoaderMock, "variables", variables );
    Field field= TeraDataBulkLoader.class.getSuperclass().getDeclaredField("variables");
    field.setAccessible(true);
    field.set( teraDataBulkLoaderMock, variables );
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
