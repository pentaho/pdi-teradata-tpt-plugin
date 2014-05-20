/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2014 by Pentaho : http://www.pentaho.com
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

import java.io.DataOutputStream;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.StreamLogger;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.steps.teradatabulkloader.TeraDataBulkLoader.TbuildThread;

/**
 * Teradata TPT Insert Upsert Bulk Loader<br>
 * <br>
 * Derived from package org.pentaho.di.trans.steps.terafast;<br>
 * Compatible with Kettle 4.4.x <br>
 * Created on 29-oct-2013<br>
 * 
 * @author Kevin Hanrahan<br>
 */

public class TeraDataBulkLoaderData extends BaseStepData implements StepDataInterface {
  public Database db;

  public int[] keynrs; // nr of keylookup -value in row...

  public StreamLogger errorLogger;

  public StreamLogger outputLogger;

  public byte[] quote = "'".getBytes();
  public byte[] separator = ",".getBytes();
  public byte[] newline;

  public ValueMetaInterface bulkTimestampMeta;
  public ValueMetaInterface bulkDateMeta;
  public ValueMetaInterface bulkNumberMeta;

  public String schemaTable;

  public String fifoFilename;

  public DataOutputStream fifoStream;

  public ValueMetaInterface[] bulkFormatMeta;

  public long bulkSize;

  public TbuildThread tbuildThread;

  /**
   * Default constructor.
   */
  public TeraDataBulkLoaderData() {
    super();

    db = null;
  }
}
