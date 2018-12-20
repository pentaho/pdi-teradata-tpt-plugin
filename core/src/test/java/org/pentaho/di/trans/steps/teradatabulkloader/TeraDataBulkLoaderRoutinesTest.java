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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;

public class TeraDataBulkLoaderRoutinesTest {

  private TeraDataBulkLoaderRoutines teraDataBulkLoaderRoutines;
  private TeraDataBulkLoader teraDataBulkLoaderMock;
  private TeraDataBulkLoaderMeta teraDataBulkLoaderMetaMock;

  @Before
  public void setup() {
    teraDataBulkLoaderMock = mock( TeraDataBulkLoader.class );
    teraDataBulkLoaderMetaMock = mock( TeraDataBulkLoaderMeta.class );
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
}
