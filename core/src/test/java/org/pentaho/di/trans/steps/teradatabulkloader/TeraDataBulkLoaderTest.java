/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.trans.steps.mock.StepMockHelper;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.matchers.JUnitMatchers.containsString;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.internal.util.reflection.Whitebox.setInternalState;

public class TeraDataBulkLoaderTest {

  private TeraDataBulkLoader teraDataBulkLoader;
  private TeraDataBulkLoaderMeta teraDataBulkLoaderMetaMock;

  private static StepMockHelper<TeraDataBulkLoaderMeta, TeraDataBulkLoaderData> helper;

  @Before
  public void setup() {
    helper =
      new StepMockHelper<>( "TeraDataBulkLoaderTest", TeraDataBulkLoaderMeta.class,
        TeraDataBulkLoaderData.class );
    when( helper.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenReturn(
      helper.logChannelInterface );
    when( helper.trans.isRunning() ).thenReturn( true );
    teraDataBulkLoaderMetaMock = mock( TeraDataBulkLoaderMeta.class );
    teraDataBulkLoader = new TeraDataBulkLoader( helper.stepMeta, helper.stepDataInterface, 0, helper.transMeta, helper.trans );
    setInternalState( teraDataBulkLoader, "meta", teraDataBulkLoaderMetaMock );
  }

  @Test
  public void createEnvironmentVariablesTest() {
    when( teraDataBulkLoaderMetaMock.getTwbRoot() ).thenReturn( "twbrootvalue" );
    when( teraDataBulkLoaderMetaMock.getCopLibPath() ).thenReturn( "coplibpathvalue" );
    when( teraDataBulkLoaderMetaMock.getLibPath() ).thenReturn( "libpathvalue" );
    when( teraDataBulkLoaderMetaMock.getTbuildLibPath() ).thenReturn( "tbuildlibpathvalue" );
    when( teraDataBulkLoaderMetaMock.getTdicuLibPath() ).thenReturn( "tdiculibpathvalue" );

    String[] envVariables = teraDataBulkLoader.createEnvironmentVariables();

    String expectedVariables = "[TWB_ROOT=twbrootvalue, COPLIB=coplibpathvalue, COPERR=coplibpathvalue, "
      + "LD_LIBRARY_PATH=libpathvalue:tbuildlibpathvalue:tdiculibpathvalue:libpathvalue64:tbuildlibpathvalue64:tdiculibpathvalue64:]";

    assertEquals( expectedVariables, Arrays.toString( envVariables ) );
  }

  @Test
  public void createEnvironmentVariablesWithEnvVarTest() {
    teraDataBulkLoader.setVariable( "TwbRoot", "twbrootvalue" );
    teraDataBulkLoader.setVariable( "CopLibPath", "coplibpathvalue" );
    teraDataBulkLoader.setVariable( "LibPath", "libpathvalue" );
    teraDataBulkLoader.setVariable( "TbuildLibPath", "tbuildlibpathvalue" );
    teraDataBulkLoader.setVariable( "TdicuLibPath", "tdiculibpathvalue" );

    when( teraDataBulkLoaderMetaMock.getTwbRoot() ).thenReturn( "${TwbRoot}" );
    when( teraDataBulkLoaderMetaMock.getCopLibPath() ).thenReturn( "${CopLibPath}" );
    when( teraDataBulkLoaderMetaMock.getLibPath() ).thenReturn( "${LibPath}" );
    when( teraDataBulkLoaderMetaMock.getTbuildLibPath() ).thenReturn( "${TbuildLibPath}" );
    when( teraDataBulkLoaderMetaMock.getTdicuLibPath() ).thenReturn( "${TdicuLibPath}" );

    String[] envVariables = teraDataBulkLoader.createEnvironmentVariables();

    String expectedVariables = "[TWB_ROOT=twbrootvalue, COPLIB=coplibpathvalue, COPERR=coplibpathvalue, "
      + "LD_LIBRARY_PATH=libpathvalue:tbuildlibpathvalue:tdiculibpathvalue:libpathvalue64:tbuildlibpathvalue64:tdiculibpathvalue64:]";

    assertEquals( expectedVariables, Arrays.toString( envVariables ) );
  }

  @Test
  public void createCommandLineTest() {
    setInternalState( teraDataBulkLoader, "tempScriptFile", "scriptValue" );
    when( teraDataBulkLoaderMetaMock.getTbuildPath() ).thenReturn( "buildpathvalue" );
    when( teraDataBulkLoaderMetaMock.getVariableFile() ).thenReturn( "variablefilevalue" );
    when( teraDataBulkLoaderMetaMock.getJobName() ).thenReturn( "jobnamevalue" );
    try {
      String commandLine = teraDataBulkLoader.createCommandLine();
      String expected = "buildpathvalue -f scriptValue -v variablefilevalue jobnamevalue";
      assertTrue( commandLine.contains( expected ) );
    } catch ( KettleException e ) {
      fail( e.getMessage() );
    }
  }

  @Test
  public void createCommandLineWithEnvVarTest() {
    teraDataBulkLoader.setVariable( "TbuilPath", "buildpathvalue" );
    teraDataBulkLoader.setVariable( "VariableFile", "variablefilevalue" );
    teraDataBulkLoader.setVariable( "JobName", "jobnamevalue" );

    setInternalState( teraDataBulkLoader, "tempScriptFile", "scriptValue" );
    when( teraDataBulkLoaderMetaMock.getTbuildPath() ).thenReturn( "${TbuilPath}" );
    when( teraDataBulkLoaderMetaMock.getVariableFile() ).thenReturn( "${VariableFile}" );
    when( teraDataBulkLoaderMetaMock.getJobName() ).thenReturn( "${JobName}" );
    try {
      String commandLine = teraDataBulkLoader.createCommandLine();
      String expected = "buildpathvalue -f scriptValue -v variablefilevalue jobnamevalue";
      assertTrue( commandLine.contains( expected ) );
    } catch ( KettleException e ) {
      fail( e.getMessage() );
    }
  }

  @Test
  public void testNoDatabaseConnection() {
    assertFalse( teraDataBulkLoader.init( helper.initStepMetaInterface, helper.initStepDataInterface ) );

    try {
      // Verify that the database connection being set to null throws a KettleException with the following message.
      teraDataBulkLoader.verifyDatabaseConnection();
      // If the method does not throw a Kettle Exception, then the DB was set and not null for this test. Fail it.
      fail( "Database Connection is not null, this fails the test." );
    } catch ( KettleException aKettleException ) {
      assertThat( aKettleException.getMessage(), containsString( "There is no connection defined in this step." ) );
    }
  }
}
