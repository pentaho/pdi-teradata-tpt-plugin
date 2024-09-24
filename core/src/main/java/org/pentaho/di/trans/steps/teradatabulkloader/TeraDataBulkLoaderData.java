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

  /** The db. */
  public Database db;

  /** nr of keylookup -value in row... */
  public int[] keynrs;

  /** The error logger. */
  public StreamLogger errorLogger;

  /** The output logger. */
  public StreamLogger outputLogger;

  /** The quote. */
  public byte[] quote = "'".getBytes();

  /** The separator. */
  public byte[] separator = ",".getBytes();

  /** The newline. */
  public byte[] newline;

  /** The bulk timestamp meta. */
  public ValueMetaInterface bulkTimestampMeta;

  /** The bulk date meta. */
  public ValueMetaInterface bulkDateMeta;

  /** The bulk number meta. */
  public ValueMetaInterface bulkNumberMeta;

  /** The schema table. */
  public String schemaTable;

  /** The fifo filename. */
  public String fifoFilename;

  /** The fifo stream. */
  public DataOutputStream fifoStream;

  /** The bulk format meta. */
  public ValueMetaInterface[] bulkFormatMeta;

  /** The bulk size. */
  public long bulkSize;

  /** The tbuild thread. */
  public TbuildThread tbuildThread;

  /**
   * Default constructor.
   */
  public TeraDataBulkLoaderData() {
    super();

    db = null;
  }
}
