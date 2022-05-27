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

import java.util.List;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.ProvidesDatabaseConnectionInformation;
import org.pentaho.di.core.SQLStatement;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.shared.SharedObjectInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/**
 * Teradata TPT Insert Upsert Bulk Loader
 */

@Step( id = "TeraDataBulkLoader", image = "BLKTDTPT.svg", name = "Teradata TPT bulk loader",
    description = "Teradata TPT bulkloader, using tbuild command",
    categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Bulk",
    documentationUrl = "https://pentaho-community.atlassian.net/wiki/display/EAI/Teradata+TPT+Insert+Upsert+Bulk+Loader" )
public class TeraDataBulkLoaderMeta extends BaseStepMeta implements StepMetaInterface,
    ProvidesDatabaseConnectionInformation {

  /** The pkg. */
  private static Class<?> PKG = TeraDataBulkLoaderMeta.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$

  /** The Constant FIELD_FORMAT_TYPE_OK. */
  public static final int FIELD_FORMAT_TYPE_OK = 0;

  /** The Constant FIELD_FORMAT_TYPE_DATE. */
  public static final int FIELD_FORMAT_TYPE_DATE = 1;

  /** The Constant FIELD_FORMAT_TYPE_TIMESTAMP. */
  public static final int FIELD_FORMAT_TYPE_TIMESTAMP = 2;

  /** The Constant FIELD_FORMAT_TYPE_NUMBER. */
  public static final int FIELD_FORMAT_TYPE_NUMBER = 3;

  /** The Constant FIELD_FORMAT_TYPE_STRING_ESCAPE. */
  public static final int FIELD_FORMAT_TYPE_STRING_ESCAPE = 4;

  /** The Constant fieldFormatTypeCodes. */
  private static final String[] fieldFormatTypeCodes = { "OK", "DATE", "TIMESTAMP", "NUMBER", "STRING_ESC" };

  /** The Constant fieldFormatTypeDescriptions. */
  private static final String[] fieldFormatTypeDescriptions = {
    BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.FieldFormatType.OK.Description" ),
    BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.FieldFormatType.Date.Description" ),
    BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.FieldFormatType.Timestamp.Description" ),
    BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.FieldFormatType.Number.Description" ),
    BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.FieldFormatType.StringEscape.Description" ), };

  public static final String CONNECTION_FIELD = "connection";
  public static final String GENERATE_SCRIPT_FIELD = "generateScript";
  public static final String TBUILD_PATH_FIELD = "tbuildPath";
  public static final String TBUILD_LIB_PATH_FIELD = "tbuildLibPath";
  public static final String LIB_PATH_FIELD = "libPath";
  public static final String TDICU_LIB_PATH_FIELD = "tdicuLibPath";
  public static final String COP_LIB_PATH_FIELD = "copLibPath";
  public static final String TWB_ROOT_FIELD = "twbRoot";
  public static final String INSTALL_PATH_FIELD = "installPath";
  public static final String JOB_NAME_FIELD = "jobName";

  public static final String SCHEMA_FIELD = "schema";
  public static final String TABLE_FIELD = "table";
  public static final String LOG_TABLE_FIELD = "logTable";
  public static final String WORK_TABLE_FIELD = "workTable";
  public static final String ERROR_TABLE_FIELD = "errorTable";
  public static final String ERROR_TABLE_2_FIELD = "errorTable2";
  public static final String DROP_LOG_TABLE_FIELD = "dropLogTable";
  public static final String DROP_WORK_TABLE_FIELD = "dropWorkTable";
  public static final String DROP_ERROR_TABLE_FIELD = "dropErrorTable";
  public static final String DROP_ERROR_TABLE_2_FIELD = "dropErrorTable2";
  public static final String IGNORE_DUP_UPDATE_FIELD = "ignoreDupUpdate";
  public static final String INSERT_MISSING_UPDATE_FIELD = "insertMissingUpdate";
  public static final String IGNORE_MISSING_UPDATE_FIELD = "ignoreMissingUpdate";
  public static final String ACCESS_LOG_FILE_FIELD = "accessLogFile";
  public static final String UPDATE_LOG_FILE_FIELD = "updateLogFile";
  public static final String FIFO_FILE_NAME_FIELD = "fifoFileName";
  public static final String RANDOMIZE_FIFO_FILE_NAME_FIELD = "randomizeFifoFilename";
  public static final String SCRIPT_FILE_NAME_FIELD = "scriptFileName";
  public static final String ACTION_TYPE_FIELD = "actionType";
  public static final String KEY_STREAM_FIELD = "keyStream";
  public static final String KEY_LOOKUP_FIELD = "keyLookup";
  public static final String KEY_CONDITION_FIELD = "keyCondition";
  public static final String FIELD_TABLE_FIELD = "fieldTable";
  public static final String FIELD_STREAM_FIELD = "fieldStream";
  public static final String FIELD_UPDATE_FIELD = "fieldUpdate";

  public static final String EXISTING_SCRIPT_FILE_FIELD = "existingScriptFile";
  public static final String SUBSTITUTE_SCRIPT_FILE_FIELD = "substituteScriptFile";
  public static final String EXISTING_VARIABLE_FILE_FIELD = "existingVariableFile";
  public static final String SUBSTITUTE_VARIABLE_FILE_FIELD = "substituteVariableFile";

  /** The my step. */
  private TeraDataBulkLoader myStep;
  /* Dialog populated variables - common */
  /** The database meta. */
  private DatabaseMeta databaseMeta;

  /** The tbuild path. */
  private String tbuildPath = null;

  /** The job name. */
  private String jobName = null;

  /** The generate script. */
  private boolean generateScript = false;

  /** The tbuild lib path. */
  private String tbuildLibPath = null;

  /** The lib path. */
  private String libPath = null;

  /** The cop lib path. */
  private String copLibPath = null;

  /** The tdicu lib path. */
  private String tdicuLibPath = null;

  /** The install path. */
  private String installPath = BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.TbuildClientRoot" );

  /** The twb root. */
  private String twbRoot = null;
  /* Dialog populated variables - use script */
  /** The existing script file. */
  private String existingScriptFile = null;

  /** The existing variable file. */
  private String existingVariableFile = null;

  /** The substitute script file. */
  private boolean substituteScriptFile = false;

  /** The substitute variable file. */
  private boolean substituteVariableFile = false;

  /* Dialog populated variables - generate script */
  /** The fifo file name. */
  private String fifoFileName = null;

  private boolean randomizeFifoFilename = false;

  /** The script file name. */
  private String scriptFileName = null;

  /** The schema name. */
  private String schemaName = null;

  /** The table name. */
  private String tableName = null;

  /** The log table. */
  private String logTable = null;

  /** The work table. */
  private String workTable = null;

  /** The error table. */
  private String errorTable = null;

  /** The error table2. */
  private String errorTable2 = null;

  /** The drop log table. */
  private boolean dropLogTable = false;

  /** The drop work table. */
  private boolean dropWorkTable = false;

  /** The drop error table. */
  private boolean dropErrorTable = false;

  /** The drop error table2. */
  private boolean dropErrorTable2 = false;

  /** The ignore dup update. */
  private boolean ignoreDupUpdate = false;

  /** The insert missing update. */
  private boolean insertMissingUpdate = false;

  /** The ignore missing update. */
  private boolean ignoreMissingUpdate = false;

  /** The access log file. */
  private String accessLogFile = null;

  /** The update log file. */
  private String updateLogFile = null;

  /** The action type. */
  private int actionType = 0; // Insert, Upsert - index into TeraDataBulkLoader.ActionTypes[]

  /** Field name of the target table. */
  private String[] fieldTable = null;

  /** Field name in the stream. */
  private String[] fieldStream = null;

  /** The field update. */
  private Boolean[] fieldUpdate = null;

  /** which field in input stream to compare with?. */
  private String[] keyStream = null;

  /** field in table. */
  private String[] keyLookup = null;

  /** Comparator: =, <>, BETWEEN, ... */
  private String[] keyCondition = null;

  /**
   * Instantiates a new tera data bulk loader meta.
   */
  public TeraDataBulkLoaderMeta() {
    super();
  }

  /**
   * Gets the database meta.
   *
   * @return Returns the database.
   */
  @Override
  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  /**
   * Gets the step.
   *
   * @return the step
   */
  public TeraDataBulkLoader getStep() {
    return this.myStep;
  }

  /**
   * Sets the generate script.
   *
   * @param val
   *          the val
   * @return the boolean
   */
  public boolean setGenerateScript( boolean val ) {
    return this.generateScript = val;
  }

  /**
   * Gets the generate script.
   *
   * @return the generate script
   */
  public boolean getGenerateScript() {
    return this.generateScript;
  }

  /**
   * Sets the substitute control file.
   *
   * @param val
   *          the val
   * @return the boolean
   */
  public boolean setSubstituteControlFile( boolean val ) {
    return this.substituteScriptFile = val;
  }

  /**
   * Gets the substitute control file.
   *
   * @return the substitute control file
   */
  public boolean getSubstituteControlFile() {
    return this.substituteScriptFile;
  }

  /**
   * Sets the existing script file.
   *
   * @param val
   *          the val
   * @return the string
   */
  public String setExistingScriptFile( String val ) {
    return this.existingScriptFile = val;
  }

  /**
   * Gets the existing script file.
   *
   * @return the existing script file
   */
  public String getExistingScriptFile() {
    return this.existingScriptFile;
  }

  /**
   * Sets the job name.
   *
   * @param val
   *          the val
   * @return the string
   */
  public String setJobName( String val ) {
    return this.jobName = val;
  }

  /**
   * Gets the job name.
   *
   * @return the job name
   */
  public String getJobName() {
    return this.jobName;
  }

  /**
   * Gets the variable file.
   *
   * @return the variable file
   */
  public String getVariableFile() {
    return this.existingVariableFile;
  }

  /**
   * Sets the variable file.
   *
   * @param val
   *          the val
   * @return the string
   */
  public String setVariableFile( String val ) {
    return this.existingVariableFile = val;
  }

  /**
   * Gets the drop log table.
   *
   * @return the drop log table
   */
  public boolean getDropLogTable() {
    return this.dropLogTable;
  }

  /**
   * Sets the drop log table.
   *
   * @param val
   *          the val
   * @return the boolean
   */
  public boolean setDropLogTable( boolean val ) {
    return this.dropLogTable = val;
  }

  /**
   * Gets the drop work table.
   *
   * @return the drop work table
   */
  public boolean getDropWorkTable() {
    return this.dropWorkTable;
  }

  /**
   * Sets the drop work table.
   *
   * @param val
   *          the val
   * @return the boolean
   */
  public boolean setDropWorkTable( boolean val ) {
    return this.dropWorkTable = val;
  }

  /**
   * Gets the drop error table.
   *
   * @return the drop error table
   */
  public boolean getDropErrorTable() {
    return this.dropErrorTable;
  }

  /**
   * Sets the drop error table.
   *
   * @param val
   *          the val
   * @return the boolean
   */
  public boolean setDropErrorTable( boolean val ) {
    return this.dropErrorTable = val;
  }

  /**
   * Gets the drop error table2.
   *
   * @return the drop error table2
   */
  public boolean getDropErrorTable2() {
    return this.dropErrorTable2;
  }

  /**
   * Sets the drop error table2.
   *
   * @param val
   *          the val
   * @return the boolean
   */
  public boolean setDropErrorTable2( boolean val ) {
    return this.dropErrorTable2 = val;
  }

  /**
   * Gets the ignore dup update.
   *
   * @return the ignore dup update
   */
  public boolean getIgnoreDupUpdate() {
    return this.ignoreDupUpdate;
  }

  /**
   * Sets the ignore dup update.
   *
   * @param val
   *          the val
   * @return the boolean
   */
  public boolean setIgnoreDupUpdate( boolean val ) {
    return this.ignoreDupUpdate = val;
  }

  /**
   * Gets the insert missing update.
   *
   * @return the insert missing update
   */
  public boolean getInsertMissingUpdate() {
    return this.insertMissingUpdate;
  }

  /**
   * Sets the insert missing update.
   *
   * @param val
   *          the val
   * @return the boolean
   */
  public boolean setInsertMissingUpdate( boolean val ) {
    return this.insertMissingUpdate = val;
  }

  /**
   * Gets the ignore missing update.
   *
   * @return the ignore missing update
   */
  public boolean getIgnoreMissingUpdate() {
    return this.ignoreMissingUpdate;
  }

  /**
   * Sets the ignore missing update.
   *
   * @param val
   *          the val
   * @return the boolean
   */
  public boolean setIgnoreMissingUpdate( boolean val ) {
    return this.ignoreMissingUpdate = val;
  }

  /**
   * Gets the substitute variable file.
   *
   * @return the substitute variable file
   */
  public boolean getSubstituteVariableFile() {
    return this.substituteVariableFile;
  }

  /**
   * Sets the substitute variable file.
   *
   * @param val
   *          the val
   * @return the boolean
   */
  public boolean setSubstituteVariableFile( boolean val ) {
    return this.substituteVariableFile = val;
  }

  /**
   * Sets the tbuild path.
   *
   * @param val
   *          the val
   * @return the string
   */
  public String setTbuildPath( String val ) {
    return this.tbuildPath = val;
  }

  /**
   * Gets the td install path.
   *
   * @return the td install path
   */
  public String getTdInstallPath() {
    return this.installPath;
  }

  /**
   * Gets the tbuild path.
   *
   * @return the tbuild path
   */
  public String getTbuildPath() {
    return Const.isEmpty( this.tbuildPath ) ? getTwbRoot() + "/bin/tbuild" : this.tbuildPath;
  }

  /**
   * Gets the tbuild lib path.
   *
   * @return the tbuild lib path
   */
  public String getTbuildLibPath() {
    return Const.isEmpty( this.tbuildLibPath ) ? getTwbRoot() + "/lib" : this.tbuildLibPath;
  }

  /**
   * Gets the lib path.
   *
   * @return the lib path
   */
  public String getLibPath() {
    return Const.isEmpty( this.libPath ) ? installPath + "/lib" : this.libPath;
  }

  /**
   * Gets the cop lib path.
   *
   * @return the cop lib path
   */
  public String getCopLibPath() {
    return Const.isEmpty( this.copLibPath ) ? installPath + "/lib" : this.copLibPath;
  }

  /**
   * Gets the tdicu lib path.
   *
   * @return the tdicu lib path
   */
  public String getTdicuLibPath() {
    return Const.isEmpty( this.tdicuLibPath ) ? installPath + "/tdicu/lib" : this.tdicuLibPath;
  }

  /**
   * Gets the twb root.
   *
   * @return the twb root
   */
  public String getTwbRoot() {
    return Const.isEmpty( this.twbRoot ) ? installPath + "/tbuild" : this.twbRoot;
  }

  /**
   * Sets the tbuild lib path.
   *
   * @param s
   *          the s
   * @return the string
   */
  public String setTbuildLibPath( String s ) {
    return this.tbuildLibPath = s;
  }

  /**
   * Sets the lib path.
   *
   * @param s
   *          the s
   * @return the string
   */
  public String setLibPath( String s ) {
    return this.libPath = s;
  }

  /**
   * Sets the cop lib path.
   *
   * @param s
   *          the s
   * @return the string
   */
  public String setCopLibPath( String s ) {
    return this.copLibPath = s;
  }

  /**
   * Sets the tdicu lib path.
   *
   * @param s
   *          the s
   * @return the string
   */
  public String setTdicuLibPath( String s ) {
    return this.tdicuLibPath = s;
  }

  /**
   * Sets the td install path.
   *
   * @param s
   *          the s
   * @return the string
   */
  public String setTdInstallPath( String s ) {
    return this.installPath = s;
  }

  /**
   * Sets the twb root.
   *
   * @param s
   *          the s
   * @return the string
   */
  public String setTwbRoot( String s ) {
    return twbRoot = s;
  }

  /**
   * Sets the fifo file name.
   *
   * @param val
   *          the val
   * @return the string
   */
  public String setFifoFileName( String val ) {
    return this.fifoFileName = val;
  }

  /**
   * Gets the fifo file name.
   *
   * @return the fifo file name
   */
  public String getFifoFileName() {
    return this.fifoFileName;
  }

  public boolean isRandomizeFifoFilename() {
    return randomizeFifoFilename;
  }

  public void setRandomizeFifoFilename( boolean randomizeFifoFilename ) {
    this.randomizeFifoFilename = randomizeFifoFilename;
  }

  /**
   * Sets the script file name.
   *
   * @param val
   *          the val
   * @return the string
   */
  public String setScriptFileName( String val ) {
    return this.scriptFileName = val;
  }

  /**
   * Gets the script file name.
   *
   * @return the script file name
   */
  public String getScriptFileName() {
    return this.scriptFileName;
  }

  /**
   * Gets the db name.
   *
   * @return the db name
   */
  public String getDbName() {
    return this.databaseMeta.getDatabaseInterface().getDatabaseName();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.di.core.ProvidesDatabaseConnectionInformation#getSchemaName()
   */
  @Override
  public String getSchemaName() {
    return this.schemaName;
  }

  /**
   * Sets the schema name.
   *
   * @param val
   *          the val
   * @return the string
   */
  public String setSchemaName( String val ) {
    return this.schemaName = val;
  }

  /**
   * Gets the log table.
   *
   * @return the log table
   */
  public String getLogTable() {
    return this.logTable;
  }

  /**
   * Sets the log table.
   *
   * @param val
   *          the val
   * @return the string
   */
  public String setLogTable( String val ) {
    return this.logTable = val;
  }

  /**
   * Gets the work table.
   *
   * @return the work table
   */
  public String getWorkTable() {
    return this.workTable;
  }

  /**
   * Sets the work table.
   *
   * @param val
   *          the val
   * @return the string
   */
  public String setWorkTable( String val ) {
    return this.workTable = val;
  }

  /**
   * Gets the error table.
   *
   * @return the error table
   */
  public String getErrorTable() {
    return this.errorTable;
  }

  /**
   * Sets the error table.
   *
   * @param val
   *          the val
   * @return the string
   */
  public String setErrorTable( String val ) {
    return this.errorTable = val;
  }

  /**
   * Gets the error table2.
   *
   * @return the error table2
   */
  public String getErrorTable2() {
    return this.errorTable2;
  }

  /**
   * Sets the error table2.
   *
   * @param val
   *          the val
   * @return the string
   */
  public String setErrorTable2( String val ) {
    return this.errorTable2 = val;
  }

  /**
   * Gets the access log file.
   *
   * @return the access log file
   */
  public String getAccessLogFile() {
    return this.accessLogFile;
  }

  /**
   * Sets the access log file.
   *
   * @param val
   *          the val
   * @return the string
   */
  public String setAccessLogFile( String val ) {
    return this.accessLogFile = val;
  }

  /**
   * Gets the update log file.
   *
   * @return the update log file
   */
  public String getUpdateLogFile() {
    return this.updateLogFile;
  }

  /**
   * Sets the update log file.
   *
   * @param val
   *          the val
   * @return the string
   */
  public String setUpdateLogFile( String val ) {
    return this.updateLogFile = val;
  }

  /**
   * Gets the action type.
   *
   * @return the action type
   */
  public int getActionType() {
    return this.actionType;
  }

  /**
   * Sets the action type.
   *
   * @param val
   *          the val
   * @return the int
   */
  public int setActionType( int val ) {
    return this.actionType = val;
  }

  /**
   * Sets the database meta.
   *
   * @param database
   *          the new database meta
   */
  public void setDatabaseMeta( DatabaseMeta database ) {
    this.databaseMeta = database;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.di.core.ProvidesDatabaseConnectionInformation#getTableName()
   */
  @Override
  public String getTableName() {
    return tableName;
  }

  /**
   * Sets the table name.
   *
   * @param tableName
   *          the new table name
   */
  public void setTableName( String tableName ) {
    this.tableName = tableName;
  }

  /**
   * Gets the field table.
   *
   * @return the field table
   */
  public String[] getFieldTable() {
    return fieldTable;
  }

  /**
   * Sets the field table.
   *
   * @param fieldTable
   *          the new field table
   */
  public void setFieldTable( String[] fieldTable ) {
    this.fieldTable = fieldTable;
  }

  /**
   * Gets the field stream.
   *
   * @return the field stream
   */
  public String[] getFieldStream() {
    return fieldStream;
  }

  /**
   * Sets the field stream.
   *
   * @param fieldStream
   *          the new field stream
   */
  public void setFieldStream( String[] fieldStream ) {
    this.fieldStream = fieldStream;
  }

  /**
   * Gets the field update.
   *
   * @return the field update
   */
  public Boolean[] getFieldUpdate() {
    return fieldUpdate;
  }

  /**
   * Sets the field update.
   *
   * @param fieldUpdate
   *          the new field update
   */
  public void setFieldUpdate( Boolean[] fieldUpdate ) {
    this.fieldUpdate = fieldUpdate;
  }

  /**
   * Gets the key stream.
   *
   * @return the key stream
   */
  public String[] getKeyStream() {
    return keyStream;
  }

  /**
   * Sets the key stream.
   *
   * @param fieldStream
   *          the new key stream
   */
  public void setKeyStream( String[] fieldStream ) {
    this.keyStream = fieldStream;
  }

  /**
   * Gets the key lookup.
   *
   * @return the key lookup
   */
  public String[] getKeyLookup() {
    return keyLookup;
  }

  /**
   * Sets the key lookup.
   *
   * @param fieldStream
   *          the new key lookup
   */
  public void setKeyLookup( String[] fieldStream ) {
    this.keyLookup = fieldStream;
  }

  /**
   * Gets the key condition.
   *
   * @return the key condition
   */
  public String[] getKeyCondition() {
    return keyCondition;
  }

  /**
   * Sets the key condition.
   *
   * @param fieldStream
   *          the new key condition
   */
  public void setKeyCondition( String[] fieldStream ) {
    this.keyCondition = fieldStream;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.di.trans.step.BaseStepMeta#loadXML(org.w3c.dom.Node, java.util.List, java.util.Map)
   */
  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    readData( stepnode, databases );
  }

  /**
   * Allocate.
   *
   * @param nrkeys
   *          the nrkeys
   * @param nrvalues
   *          the nrvalues
   */
  public void allocate( int nrkeys, int nrvalues ) {
    keyStream = new String[nrkeys];
    keyLookup = new String[nrkeys];
    keyCondition = new String[nrkeys];
    fieldTable = new String[nrvalues];
    fieldStream = new String[nrvalues];
    fieldUpdate = new Boolean[nrvalues];
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.di.trans.step.BaseStepMeta#clone()
   */
  @Override
  public Object clone() {
    TeraDataBulkLoaderMeta retval = (TeraDataBulkLoaderMeta) super.clone();
    int nrvalues = fieldTable.length;
    int nrkeys = keyStream.length;

    retval.allocate( nrkeys, nrvalues );

    for ( int i = 0; i < nrkeys; i++ ) {
      retval.keyStream[i] = keyStream[i];
      retval.keyLookup[i] = keyLookup[i];
      retval.keyCondition[i] = keyCondition[i];
    }

    for ( int i = 0; i < nrvalues; i++ ) {
      retval.fieldTable[i] = fieldTable[i];
      retval.fieldStream[i] = fieldStream[i];
      retval.fieldUpdate[i] = fieldUpdate[i];
    }
    return retval;
  }

  /**
   * Read data.
   *
   * @param stepnode
   *          the stepnode
   * @param databases
   *          the databases
   * @throws KettleXMLException
   *           the kettle xml exception
   */
  private void readData( Node stepnode, List<? extends SharedObjectInterface> databases ) throws KettleXMLException {
    try {
      // common options
      tbuildPath = XMLHandler.getTagValue( stepnode, "tbuildPath" );
      tbuildLibPath = XMLHandler.getTagValue( stepnode, "tbuildLibPath" );
      libPath = XMLHandler.getTagValue( stepnode, "libPath" );
      copLibPath = XMLHandler.getTagValue( stepnode, "copLibPath" );
      tdicuLibPath = XMLHandler.getTagValue( stepnode, "tdicuLibPath" );
      installPath = XMLHandler.getTagValue( stepnode, "installPath" );
      twbRoot = XMLHandler.getTagValue( stepnode, "twbRoot" );

      jobName = XMLHandler.getTagValue( stepnode, "jobName" );

      String con = XMLHandler.getTagValue( stepnode, "connection" );
      databaseMeta = DatabaseMeta.findDatabase( databases, con );
      generateScript = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "generateScript" ) );

      // generate script options
      schemaName = XMLHandler.getTagValue( stepnode, "schema" );
      tableName = XMLHandler.getTagValue( stepnode, "table" );
      logTable = XMLHandler.getTagValue( stepnode, "logTable" );
      workTable = XMLHandler.getTagValue( stepnode, "workTable" );
      errorTable = XMLHandler.getTagValue( stepnode, "errorTable" );
      errorTable2 = XMLHandler.getTagValue( stepnode, "errorTable2" );
      dropLogTable = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "dropLogTable" ) );
      dropWorkTable = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "dropWorkTable" ) );
      dropErrorTable = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "dropErrorTable" ) );
      dropErrorTable2 = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "dropErrorTable2" ) );
      ignoreDupUpdate = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "ignoreDupUpdate" ) );
      insertMissingUpdate = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "insertMissingUpdate" ) );
      ignoreMissingUpdate = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "ignoreMissingUpdate" ) );

      accessLogFile = XMLHandler.getTagValue( stepnode, "accessLogFile" );
      updateLogFile = XMLHandler.getTagValue( stepnode, "updateLogFile" );
      fifoFileName = XMLHandler.getTagValue( stepnode, "fifoFileName" );
      randomizeFifoFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, RANDOMIZE_FIFO_FILE_NAME_FIELD ) );
      scriptFileName = XMLHandler.getTagValue( stepnode, "scriptFileName" );
      String sactionType = XMLHandler.getTagValue( stepnode, "actionType" );
      if ( sactionType != null ) {
        actionType = Integer.parseInt( sactionType );
      }

      // Use existing file option
      existingScriptFile = XMLHandler.getTagValue( stepnode, "existingScriptFile" );
      substituteScriptFile = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "substituteScriptFile" ) );
      existingVariableFile = XMLHandler.getTagValue( stepnode, "existingVariableFile" );
      substituteVariableFile = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "substituteVariableFile" ) );

      Node lookup = XMLHandler.getSubNode( stepnode, "lookup" );
      int nrvalues = XMLHandler.countNodes( stepnode, "mapping" );
      int nrkeys = XMLHandler.countNodes( lookup, "key" );

      allocate( nrkeys, nrvalues );
      for ( int i = 0; i < nrkeys; i++ ) {
        Node knode = XMLHandler.getSubNodeByNr( lookup, "key", i );
        keyStream[i] = XMLHandler.getTagValue( knode, "name" );
        keyLookup[i] = XMLHandler.getTagValue( knode, "field" );
        keyCondition[i] = XMLHandler.getTagValue( knode, "condition" );
        if ( keyCondition[i] == null ) {
          keyCondition[i] = "=";
        }
      }

      for ( int i = 0; i < nrvalues; i++ ) {
        Node vnode = XMLHandler.getSubNodeByNr( stepnode, "mapping", i );

        fieldTable[i] = XMLHandler.getTagValue( vnode, "stream_name" );
        fieldStream[i] = XMLHandler.getTagValue( vnode, "field_name" );
        if ( fieldStream[i] == null ) {
          fieldStream[i] = fieldTable[i]; // default: the same name!
        }
        String updateValue = XMLHandler.getTagValue( vnode, "update" );
        if ( updateValue == null ) {
          // default TRUE
          fieldUpdate[i] = Boolean.TRUE;
        } else {
          if ( updateValue.equalsIgnoreCase( "Y" ) ) {
            fieldUpdate[i] = Boolean.TRUE;
          } else {
            fieldUpdate[i] = Boolean.FALSE;
          }
        }
      }
    } catch ( Exception e ) {
      throw new KettleXMLException( BaseMessages.getString( PKG,
          "TeraDataBulkLoaderMeta.Exception.UnableToReadStepInfoFromXML" ), e );
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.di.trans.step.StepMetaInterface#setDefault()
   */
  @Override
  public void setDefault() {
    fieldTable = null;
    databaseMeta = null;
    generateScript = true;
    allocate( 0, 0 );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.di.trans.step.BaseStepMeta#getXML()
   */
  @Override
  public String getXML() {
    StringBuffer retval = new StringBuffer( 3000 );

    // common
    retval
        .append( "    " ).append( XMLHandler.addTagValue( "connection", databaseMeta == null ? "" : databaseMeta.getName() ) ); //$NON-NLS-3$
    retval.append( "    " ).append( XMLHandler.addTagValue( "generateScript", generateScript ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "tbuildPath", tbuildPath ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "tbuildLibPath", tbuildLibPath ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "libPath", libPath ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "tdicuLibPath", tdicuLibPath ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "copLibPath", copLibPath ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "twbRoot", twbRoot ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "installPath", installPath ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "jobName", jobName ) );

    // generate options
    retval.append( "    " ).append( XMLHandler.addTagValue( "schema", schemaName ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "table", tableName ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "logTable", logTable ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "workTable", workTable ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "errorTable", errorTable ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "errorTable2", errorTable2 ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "dropLogTable", dropLogTable ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "dropWorkTable", dropWorkTable ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "dropErrorTable", dropErrorTable ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "dropErrorTable2", dropErrorTable2 ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "ignoreDupUpdate", ignoreDupUpdate ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "insertMissingUpdate", insertMissingUpdate ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "ignoreMissingUpdate", ignoreMissingUpdate ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "accessLogFile", accessLogFile ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "updateLogFile", updateLogFile ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "fifoFileName", fifoFileName ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( RANDOMIZE_FIFO_FILE_NAME_FIELD, randomizeFifoFilename ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "scriptFileName", scriptFileName ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "actionType", actionType ) );

    // pre-existing options
    retval.append( "    " ).append( XMLHandler.addTagValue( "existingScriptFile", existingScriptFile ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "substituteScriptFile", substituteScriptFile ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "existingVariableFile", existingVariableFile ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "substituteVariableFile", substituteVariableFile ) );

    retval.append( "    <lookup>" ).append( Const.CR );
    for ( int i = 0; i < keyStream.length; i++ ) {
      retval.append( "      <key>" ).append( Const.CR );
      retval.append( "        " ).append( XMLHandler.addTagValue( "name", keyStream[i] ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "field", keyLookup[i] ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "condition", keyCondition[i] ) );
      retval.append( "      </key>" ).append( Const.CR );
    }
    retval.append( "    </lookup>" ).append( Const.CR );

    for ( int i = 0; i < fieldTable.length; i++ ) {
      retval.append( "      <mapping>" ).append( Const.CR );
      retval.append( "        " ).append( XMLHandler.addTagValue( "stream_name", fieldTable[i] ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "field_name", fieldStream[i] ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "update", fieldUpdate[i].booleanValue() ) );
      retval.append( "      </mapping>" ).append( Const.CR );
    }

    return retval.toString();
  }

  /**
   * Read step metadata from repository
   */
  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {

    try {
      databaseMeta = rep.loadDatabaseMetaFromStepAttribute( id_step, "id_connection", databases );
      tableName = rep.getStepAttributeString( id_step, "table" );

      int nrkeys = rep.countNrStepAttributes( id_step, "key_field" );
      int nrvalues = rep.countNrStepAttributes( id_step, "stream_name" );

      allocate( nrkeys, nrvalues );

      for ( int i = 0; i < nrkeys; i++ ) {
        keyStream[i] = rep.getStepAttributeString( id_step, i, "key_name" );
        keyLookup[i] = rep.getStepAttributeString( id_step, i, "key_field" );
        keyCondition[i] = rep.getStepAttributeString( id_step, i, "key_condition" );
      }

      for ( int i = 0; i < nrvalues; i++ ) {
        fieldTable[i] = rep.getStepAttributeString( id_step, i, "stream_name" );
        fieldStream[i] = rep.getStepAttributeString( id_step, i, "field_name" );
        if ( fieldStream[i] == null ) {
          fieldStream[i] = fieldTable[i];
        }
        fieldUpdate[i] = Boolean.valueOf( rep.getStepAttributeBoolean( id_step, i, "value_update", true ) );
      }

      generateScript = rep.getStepAttributeBoolean( id_step, "generateScript" );
      tbuildPath = rep.getStepAttributeString( id_step, "tbuildPath" );
      tbuildLibPath = rep.getStepAttributeString( id_step, "tbuildLibPath" );
      libPath = rep.getStepAttributeString( id_step, "libPath" );
      tdicuLibPath = rep.getStepAttributeString( id_step, "tdicuLibPath" );
      copLibPath = rep.getStepAttributeString( id_step, "copLibPath" );
      twbRoot = rep.getStepAttributeString( id_step, "twbRoot" );
      installPath = rep.getStepAttributeString( id_step, "installPath" );
      jobName = rep.getStepAttributeString( id_step, "jobName" );
      fifoFileName = rep.getStepAttributeString( id_step, "fifoFileName" );
      randomizeFifoFilename = rep.getStepAttributeBoolean( id_step, RANDOMIZE_FIFO_FILE_NAME_FIELD );

      schemaName = rep.getStepAttributeString( id_step, "schema" );
      tableName = rep.getStepAttributeString( id_step, "table" );
      logTable = rep.getStepAttributeString( id_step, "logTable" );
      workTable = rep.getStepAttributeString( id_step, "workTable" );
      errorTable = rep.getStepAttributeString( id_step, "errorTable" );
      errorTable2 = rep.getStepAttributeString( id_step, "errorTable2" );
      dropLogTable = rep.getStepAttributeBoolean( id_step, "dropLogTable" );
      dropWorkTable = rep.getStepAttributeBoolean( id_step, "dropWorkTable" );
      dropErrorTable = rep.getStepAttributeBoolean( id_step, "dropErrorTable" );
      dropErrorTable2 = rep.getStepAttributeBoolean( id_step, "dropErrorTable2" );
      ignoreDupUpdate = rep.getStepAttributeBoolean( id_step, "ignoreDupUpdate" );
      insertMissingUpdate = rep.getStepAttributeBoolean( id_step, "insertMissingUpdate" );
      ignoreMissingUpdate = rep.getStepAttributeBoolean( id_step, "ignoreMissingUpdate" );
      accessLogFile = rep.getStepAttributeString( id_step, "accessLogFile" );
      updateLogFile = rep.getStepAttributeString( id_step, "updateLogFile" );
      scriptFileName = rep.getStepAttributeString( id_step, "scriptFileName" );
      actionType = (int) rep.getStepAttributeInteger( id_step, "actionType" );

      existingScriptFile = rep.getStepAttributeString( id_step, "existingScriptFile" );
      substituteScriptFile = rep.getStepAttributeBoolean( id_step, "substituteScriptFile" );
      existingVariableFile = rep.getStepAttributeString( id_step, "existingVariableFile" );
      substituteVariableFile = rep.getStepAttributeBoolean( id_step, "substituteVariableFile" );
    } catch ( Exception e ) {
      throw new KettleException( BaseMessages.getString( PKG,
          "TeraDataBulkLoaderMeta.Exception.UnexpectedErrorReadingStepInfoFromRepository" ), e );
    }
  }

  /**
   * Save step metadata to repository
   */
  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
    throws KettleException {

    // 4.x compatibility

    try {
      rep.saveDatabaseMetaStepAttribute( id_transformation, id_step, "id_connection", databaseMeta );
      rep.saveStepAttribute( id_transformation, id_step, "table", tableName );

      for ( int i = 0; i < keyStream.length; i++ ) {
        rep.saveStepAttribute( id_transformation, id_step, i, "key_name", keyStream[i] );
        rep.saveStepAttribute( id_transformation, id_step, i, "key_field", keyLookup[i] );
        rep.saveStepAttribute( id_transformation, id_step, i, "key_condition", keyCondition[i] );
      }
      for ( int i = 0; i < fieldTable.length; i++ ) {
        rep.saveStepAttribute( id_transformation, id_step, i, "stream_name", fieldTable[i] );
        rep.saveStepAttribute( id_transformation, id_step, i, "field_name", fieldStream[i] );
        rep.saveStepAttribute( id_transformation, id_step, i, "value_update", fieldUpdate[i].booleanValue() );
      }

      // Also, save the step-database relationship!
      if ( databaseMeta != null ) {
        rep.insertStepDatabase( id_transformation, id_step, databaseMeta.getObjectId() );
      }
      rep.saveStepAttribute( id_transformation, id_step, "generateScript", generateScript );
      rep.saveStepAttribute( id_transformation, id_step, "tbuildPath", tbuildPath );
      rep.saveStepAttribute( id_transformation, id_step, "tbuildLibPath", tbuildLibPath );
      rep.saveStepAttribute( id_transformation, id_step, "libPath", libPath );
      rep.saveStepAttribute( id_transformation, id_step, "tdicuLibPath", tdicuLibPath );
      rep.saveStepAttribute( id_transformation, id_step, "copLibPath", copLibPath );
      rep.saveStepAttribute( id_transformation, id_step, "twbRoot", twbRoot );
      rep.saveStepAttribute( id_transformation, id_step, "installPath", installPath );
      rep.saveStepAttribute( id_transformation, id_step, "jobName", jobName );
      rep.saveStepAttribute( id_transformation, id_step, "fifoFileName", fifoFileName );
      rep.saveStepAttribute( id_transformation, id_step, RANDOMIZE_FIFO_FILE_NAME_FIELD, randomizeFifoFilename );

      rep.saveStepAttribute( id_transformation, id_step, "schema", schemaName );
      rep.saveStepAttribute( id_transformation, id_step, "logTable", logTable );
      rep.saveStepAttribute( id_transformation, id_step, "workTable", workTable );
      rep.saveStepAttribute( id_transformation, id_step, "errorTable", errorTable );
      rep.saveStepAttribute( id_transformation, id_step, "errorTable2", errorTable2 );
      rep.saveStepAttribute( id_transformation, id_step, "dropLogTable", dropLogTable );
      rep.saveStepAttribute( id_transformation, id_step, "dropWorkTable", dropWorkTable );
      rep.saveStepAttribute( id_transformation, id_step, "dropErrorTable", dropErrorTable );
      rep.saveStepAttribute( id_transformation, id_step, "dropErrorTable2", dropErrorTable2 );
      rep.saveStepAttribute( id_transformation, id_step, "ignoreDupUpdate", ignoreDupUpdate );
      rep.saveStepAttribute( id_transformation, id_step, "insertMissingUpdate", insertMissingUpdate );
      rep.saveStepAttribute( id_transformation, id_step, "ignoreMissingUpdate", ignoreMissingUpdate );
      rep.saveStepAttribute( id_transformation, id_step, "accessLogFile", accessLogFile );
      rep.saveStepAttribute( id_transformation, id_step, "updateLogFile", updateLogFile );
      rep.saveStepAttribute( id_transformation, id_step, "scriptFileName", scriptFileName );
      rep.saveStepAttribute( id_transformation, id_step, "actionType", actionType );

      rep.saveStepAttribute( id_transformation, id_step, "existingScriptFile", existingScriptFile );
      rep.saveStepAttribute( id_transformation, id_step, "substituteScriptFile", substituteScriptFile );
      rep.saveStepAttribute( id_transformation, id_step, "existingVariableFile", existingVariableFile );
      rep.saveStepAttribute( id_transformation, id_step, "substituteVariableFile", substituteVariableFile );
    } catch ( Exception e ) {
      throw new KettleException( BaseMessages.getString( PKG,
          "TeraDataBulkLoaderMeta.Exception.UnableToSaveStepInfoToRepository" )
          + id_step, e );
    }
  }

  /**
   * Check the values stored in this metadata
   */
  @Override
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
      String[] input, String[] output, RowMetaInterface arg6 ) {

    CheckResult cr;
    String error_message = "";

    if ( databaseMeta != null ) {
      Database db = new Database( loggingObject, databaseMeta );
      db.shareVariablesWith( transMeta );
      try {
        db.connect();

        if ( !Const.isEmpty( tableName ) ) {
          cr =
              new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
                  "TeraDataBulkLoaderMeta.CheckResult.TableNameOK" ), stepMeta );
          remarks.add( cr );

          boolean first = true;
          boolean error_found = false;
          error_message = "";

          // Check fields in table
          String schemaTable =
              databaseMeta.getQuotedSchemaTableCombination( transMeta.environmentSubstitute( schemaName ), transMeta
                  .environmentSubstitute( tableName ) );
          RowMetaInterface r = db.getTableFields( schemaTable );
          if ( r != null ) {
            cr =
                new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
                    "TeraDataBulkLoaderMeta.CheckResult.TableExists" ), stepMeta );
            remarks.add( cr );

            // How about the fields to insert/dateMask in the table?
            first = true;
            error_found = false;
            error_message = "";

            for ( int i = 0; i < fieldTable.length; i++ ) {
              String field = fieldTable[i];

              ValueMetaInterface v = r.searchValueMeta( field );
              if ( v == null ) {
                if ( first ) {
                  first = false;
                  error_message +=
                      BaseMessages.getString( PKG,
                          "TeraDataBulkLoaderMeta.CheckResult.MissingFieldsToLoadInTargetTable" )
                          + Const.CR;
                }
                error_found = true;
                error_message += "\t\t" + field + Const.CR;
              }
            }
            if ( error_found ) {
              cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta );
            } else {
              cr =
                  new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
                      "TeraDataBulkLoaderMeta.CheckResult.AllFieldsFoundInTargetTable" ), stepMeta );
            }
            remarks.add( cr );
          } else {
            error_message = BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.CheckResult.CouldNotReadTableInfo" );
            cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta );
            remarks.add( cr );
          }
        }

        // Look up fields in the input stream <prev>
        if ( prev != null && prev.size() > 0 ) {
          cr =
              new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
                  "TeraDataBulkLoaderMeta.CheckResult.StepReceivingDatas", prev.size() + "" ), stepMeta );
          remarks.add( cr );

          boolean first = true;
          error_message = "";
          boolean error_found = false;

          for ( int i = 0; i < fieldStream.length; i++ ) {
            ValueMetaInterface v = prev.searchValueMeta( fieldStream[i] );
            if ( v == null ) {
              if ( first ) {
                first = false;
                error_message +=
                    BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.CheckResult.MissingFieldsInInput" ) + Const.CR;
              }
              error_found = true;
              error_message += "\t\t" + fieldStream[i] + Const.CR;
            }
          }
          if ( error_found ) {
            cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta );
          } else {
            cr =
                new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
                    "TeraDataBulkLoaderMeta.CheckResult.AllFieldsFoundInInput" ), stepMeta );
          }
          remarks.add( cr );
        } else {
          error_message =
              BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.CheckResult.MissingFieldsInInput3" ) + Const.CR;
          cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta );
          remarks.add( cr );
        }
      } catch ( KettleException e ) {
        error_message =
            BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.CheckResult.DatabaseErrorOccurred" ) + e.getMessage();
        cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta );
        remarks.add( cr );
      } finally {
        db.disconnect();
      }
    } else {
      error_message = BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.CheckResult.InvalidConnection" );
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
              "TeraDataBulkLoaderMeta.CheckResult.StepReceivingInfoFromOtherSteps" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
              "TeraDataBulkLoaderMeta.CheckResult.NoInputError" ), stepMeta );
      remarks.add( cr );
    }
  }

  /**
   * Gets the SQL statements.
   *
   * @param transMeta
   *          the trans meta
   * @param stepMeta
   *          the step meta
   * @param prev
   *          the prev
   * @param repository
   *          the repository
   * @return the SQL statements
   * @throws KettleStepException
   *           the kettle step exception
   */
  public SQLStatement getSQLStatements( TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
      Repository repository ) throws KettleStepException {
    SQLStatement retval = new SQLStatement( stepMeta.getName(), databaseMeta, null ); // default: nothing to do!

    if ( databaseMeta != null ) {
      if ( prev != null && prev.size() > 0 ) {
        // Copy the row
        RowMetaInterface tableFields = new RowMeta();

        // Now change the field names
        for ( int i = 0; i < fieldTable.length; i++ ) {
          ValueMetaInterface v = prev.searchValueMeta( fieldStream[i] );
          if ( v != null ) {
            ValueMetaInterface tableField = v.clone();
            tableField.setName( fieldTable[i] );
            tableFields.addValueMeta( tableField );
          } else {
            throw new KettleStepException( BaseMessages.getString( PKG,
                "TeraDataBulkLoaderMeta.Exception.CannotFindFieldInInput", fieldStream[i] ) );
          }
        }

        if ( !Const.isEmpty( tableName ) ) {
          Database db = new Database( loggingObject, databaseMeta );
          db.shareVariablesWith( transMeta );
          try {
            db.connect();

            String schemaTable =
                databaseMeta.getQuotedSchemaTableCombination( transMeta.environmentSubstitute( schemaName ), transMeta
                    .environmentSubstitute( tableName ) );
            String cr_table = db.getDDL( schemaTable, tableFields, null, false, null, true );

            String sql = cr_table;
            if ( sql.length() == 0 ) {
              retval.setSQL( null );
            } else {
              retval.setSQL( sql );
            }
          } catch ( KettleException e ) {
            retval.setError( BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.GetSQL.ErrorOccurred" )
                + e.getMessage() );
          }
        } else {
          retval.setError( BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.GetSQL.NoTableDefinedOnConnection" ) );
        }
      } else {
        retval.setError( BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.GetSQL.NotReceivingAnyFields" ) );
      }
    } else {
      retval.setError( BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.GetSQL.NoConnectionDefined" ) );
    }

    return retval;
  }

  /**
   * Get the StepInterface associated with this step metadata
   */
  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
      Trans trans ) {
    myStep = new TeraDataBulkLoader( stepMeta, stepDataInterface, cnr, transMeta, trans );
    return myStep;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.di.trans.step.StepMetaInterface#getStepData()
   */
  @Override
  public StepDataInterface getStepData() {
    return new TeraDataBulkLoaderData();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.di.trans.step.BaseStepMeta#getUsedDatabaseConnections()
   */
  @Override
  public DatabaseMeta[] getUsedDatabaseConnections() {
    if ( databaseMeta != null ) {
      return new DatabaseMeta[] { databaseMeta };
    } else {
      return super.getUsedDatabaseConnections();
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.di.trans.step.BaseStepMeta#getRequiredFields(org.pentaho.di.core.variables.VariableSpace)
   */
  @Override
  public RowMetaInterface getRequiredFields( VariableSpace space ) throws KettleException {
    String realTableName = space.environmentSubstitute( tableName );
    String realSchemaName = space.environmentSubstitute( schemaName );

    if ( databaseMeta != null ) {
      Database db = new Database( loggingObject, databaseMeta );
      try {
        db.connect();

        if ( !Const.isEmpty( realTableName ) ) {
          String schemaTable = databaseMeta.getQuotedSchemaTableCombination( realSchemaName, realTableName );
          // Check if this table exists...
          if ( db.checkTableExists( schemaTable ) ) {
            RowMetaInterface rv = db.getTableFields( schemaTable );
            return rv;
          } else {
            throw new KettleException( BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.Exception.TableNotFound" ) );
          }
        } else {
          throw new KettleException( BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.Exception.TableNotSpecified" ) );
        }
      } catch ( Exception e ) {
        throw new KettleException(
            BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.Exception.ErrorGettingFields" ), e );
      } finally {
        db.disconnect();
      }
    } else {
      throw new KettleException( BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.Exception.ConnectionNotDefined" ) );
    }

  }

  /**
   * Gets the field format type codes.
   *
   * @return the schemaName
   */

  public static String[] getFieldFormatTypeCodes() {
    return fieldFormatTypeCodes;
  }

  /**
   * Gets the field format type descriptions.
   *
   * @return the field format type descriptions
   */
  public static String[] getFieldFormatTypeDescriptions() {
    return fieldFormatTypeDescriptions;
  }

  /**
   * Gets the field format type code.
   *
   * @param type
   *          the type
   * @return the field format type code
   */
  public static String getFieldFormatTypeCode( int type ) {
    return fieldFormatTypeCodes[type];
  }

  /**
   * Gets the field format type description.
   *
   * @param type
   *          the type
   * @return the field format type description
   */
  public static String getFieldFormatTypeDescription( int type ) {
    return fieldFormatTypeDescriptions[type];
  }

  /**
   * Gets the field format type.
   *
   * @param codeOrDescription
   *          the code or description
   * @return the field format type
   */
  public static int getFieldFormatType( String codeOrDescription ) {
    for ( int i = 0; i < fieldFormatTypeCodes.length; i++ ) {
      if ( fieldFormatTypeCodes[i].equalsIgnoreCase( codeOrDescription ) ) {
        return i;
      }
    }
    for ( int i = 0; i < fieldFormatTypeDescriptions.length; i++ ) {
      if ( fieldFormatTypeDescriptions[i].equalsIgnoreCase( codeOrDescription ) ) {
        return i;
      }
    }
    return FIELD_FORMAT_TYPE_OK;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.di.core.ProvidesDatabaseConnectionInformation#getMissingDatabaseConnectionInformationMessage()
   */
  @Override
  public String getMissingDatabaseConnectionInformationMessage() {
    return null;
  }

}
