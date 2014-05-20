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

import java.util.List;
import java.util.Map;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.ProvidesDatabaseConnectionInformation;
import org.pentaho.di.core.SQLStatement;
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
import org.w3c.dom.Node;
import org.pentaho.di.core.annotations.Step;

/**
 * Teradata TPT Insert Upsert Bulk Loader<br>
 * <br>
 * Derived from package org.pentaho.di.trans.steps.terafast;<br>
 * Compatible with Kettle 4.4.x <br>
 * Created on 29-oct-2013<br>
 * 
 * @author Kevin Hanrahan<br>
 */

@Step( id = "TeraDataBulkLoader", image = "TDTPTBL.png", name = "Teradata TPT Insert Upsert Bulk Loader",
    description = "Teradata TPT bulkloader, using tbuild command",
    categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Bulk",
    documentationUrl = "http://wiki.pentaho.com/display/EAI/Teradata+TPT+Insert+Upsert+Bulk+Loader" )
public class TeraDataBulkLoaderMeta extends BaseStepMeta implements StepMetaInterface,
    ProvidesDatabaseConnectionInformation {
  private static Class<?> PKG = TeraDataBulkLoaderMeta.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$

  public static final int FIELD_FORMAT_TYPE_OK = 0;
  public static final int FIELD_FORMAT_TYPE_DATE = 1;
  public static final int FIELD_FORMAT_TYPE_TIMESTAMP = 2;
  public static final int FIELD_FORMAT_TYPE_NUMBER = 3;
  public static final int FIELD_FORMAT_TYPE_STRING_ESCAPE = 4;

  private static final String[] fieldFormatTypeCodes = { "OK", "DATE", "TIMESTAMP", "NUMBER", "STRING_ESC" };
  private static final String[] fieldFormatTypeDescriptions = {
    BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.FieldFormatType.OK.Description" ),
    BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.FieldFormatType.Date.Description" ),
    BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.FieldFormatType.Timestamp.Description" ),
    BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.FieldFormatType.Number.Description" ),
    BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.FieldFormatType.StringEscape.Description" ), };

  private TeraDataBulkLoader myStep;
  /* Dialog populated variables - common */
  private DatabaseMeta databaseMeta;
  private String tbuildPath = null;
  private String jobName = null;
  private Boolean generateScript = null;
  private String tbuildLibPath = null;
  private String libPath = null;
  private String copLibPath = null;
  private String tdicuLibPath = null;
  private String installPath = BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.TbuildClientRoot" );
  private String twbRoot = null;
  /* Dialog populated variables - use script */
  private String existingScriptFile = null;
  private String existingVariableFile = null;
  private Boolean substituteScriptFile = null;
  private Boolean substituteVariableFile = null;

  /* Dialog populated variables - generate script */
  private String fifoFileName = null;
  private String scriptFileName = null;
  private String schemaName = null;
  private String tableName = null;
  private String logTable = null;
  private String workTable = null;
  private String errorTable = null;
  private String errorTable2 = null;
  private Boolean dropLogTable = false;
  private Boolean dropWorkTable = false;
  private Boolean dropErrorTable = false;
  private Boolean dropErrorTable2 = false;
  private Boolean ignoreDupUpdate = false;
  private Boolean insertMissingUpdate = false;
  private Boolean ignoreMissingUpdate = false;
  private String accessLogFile = null;
  private String updateLogFile = null;
  private int actionType = 0; // Insert, Upsert - index into TeraDataBulkLoader.ActionTypes[]

  /** Field name of the target table */
  private String[] fieldTable = null;

  /** Field name in the stream */
  private String[] fieldStream = null;
  private Boolean[] fieldUpdate = null;
  /** which field in input stream to compare with? */
  private String[] keyStream = null;

  /** field in table */
  private String[] keyLookup = null;

  /** Comparator: =, <>, BETWEEN, ... */
  private String[] keyCondition = null;

  public TeraDataBulkLoaderMeta() {
    super();
  }

  /**
   * @return Returns the database.
   */
  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  public TeraDataBulkLoader getStep() {
    return this.myStep;
  }

  public Boolean setGenerateScript( Boolean val ) {
    return this.generateScript = val;
  }

  public Boolean getGenerateScript() {
    return this.generateScript;
  }

  public Boolean setSubstituteControlFile( Boolean val ) {
    return this.substituteScriptFile = val;
  }

  public Boolean getSubstituteControlFile() {
    return this.substituteScriptFile;
  }

  public String setExistingScriptFile( String val ) {
    return this.existingScriptFile = val;
  }

  public String getExistingScriptFile() {
    return this.existingScriptFile;
  }

  public String setJobName( String val ) {
    return this.jobName = val;
  }

  public String getJobName() {
    return this.jobName;
  }

  public String getVariableFile() {
    return this.existingVariableFile;
  }

  public String setVariableFile( String val ) {
    return this.existingVariableFile = val;
  }

  public Boolean getDropLogTable() {
    return this.dropLogTable;
  }

  public Boolean setDropLogTable( Boolean val ) {
    return this.dropLogTable = val;
  }

  public Boolean getDropWorkTable() {
    return this.dropWorkTable;
  }

  public Boolean setDropWorkTable( Boolean val ) {
    return this.dropWorkTable = val;
  }

  public Boolean getDropErrorTable() {
    return this.dropErrorTable;
  }

  public Boolean setDropErrorTable( Boolean val ) {
    return this.dropErrorTable = val;
  }

  public Boolean getDropErrorTable2() {
    return this.dropErrorTable2;
  }

  public Boolean setDropErrorTable2( Boolean val ) {
    return this.dropErrorTable2 = val;
  }

  public Boolean getIgnoreDupUpdate() {
    return this.ignoreDupUpdate;
  }

  public Boolean setIgnoreDupUpdate( Boolean val ) {
    return this.ignoreDupUpdate = val;
  }

  public Boolean getInsertMissingUpdate() {
    return this.insertMissingUpdate;
  }

  public Boolean setInsertMissingUpdate( Boolean val ) {
    return this.insertMissingUpdate = val;
  }

  public Boolean getIgnoreMissingUpdate() {
    return this.ignoreMissingUpdate;
  }

  public Boolean setIgnoreMissingUpdate( Boolean val ) {
    return this.ignoreMissingUpdate = val;
  }

  public Boolean getSubstituteVariableFile() {
    return this.substituteVariableFile;
  }

  public Boolean setSubstituteVariableFile( Boolean val ) {
    return this.substituteVariableFile = val;
  }

  public String setTbuildPath( String val ) {
    return this.tbuildPath = val;
  }

  public String getTdInstallPath() {
    return this.installPath;
  }

  public String getTbuildPath() {
    return Const.isEmpty( this.tbuildPath ) ? getTwbRoot() + "/bin/tbuild" : this.tbuildPath;
  }

  public String getTbuildLibPath() {
    return Const.isEmpty( this.tbuildLibPath ) ? getTwbRoot() + "/lib" : this.tbuildLibPath;
  }

  public String getLibPath() {
    return Const.isEmpty( this.libPath ) ? installPath + "/lib" : this.libPath;
  }

  public String getCopLibPath() {
    return Const.isEmpty( this.copLibPath ) ? installPath + "/lib" : this.copLibPath;
  }

  public String getTdicuLibPath() {
    return Const.isEmpty( this.tdicuLibPath ) ? installPath + "/tdicu/lib" : this.tdicuLibPath;
  }

  public String getTwbRoot() {
    return Const.isEmpty( this.twbRoot ) ? installPath + "/tbuild" : this.twbRoot;
  }

  public String setTbuildLibPath( String s ) {
    return this.tbuildLibPath = s;
  }

  public String setLibPath( String s ) {
    return this.libPath = s;
  }

  public String setCopLibPath( String s ) {
    return this.copLibPath = s;
  }

  public String setTdicuLibPath( String s ) {
    return this.tdicuLibPath = s;
  }

  public String setTdInstallPath( String s ) {
    return this.installPath = s;
  }

  public String setTwbRoot( String s ) {
    return twbRoot = s;
  }

  public String setFifoFileName( String val ) {
    return this.fifoFileName = val;
  }

  public String getFifoFileName() {
    return this.fifoFileName;
  }

  public String setScriptFileName( String val ) {
    return this.scriptFileName = val;
  }

  public String getScriptFileName() {
    return this.scriptFileName;
  }

  public String getDbName() {
    return this.databaseMeta.getDatabaseInterface().getDatabaseName();
  }

  public String getSchemaName() {
    return this.schemaName;
  }

  public String setSchemaName( String val ) {
    return this.schemaName = val;
  }

  public String getLogTable() {
    return this.logTable;
  }

  public String setLogTable( String val ) {
    return this.logTable = val;
  }

  public String getWorkTable() {
    return this.workTable;
  }

  public String setWorkTable( String val ) {
    return this.workTable = val;
  }

  public String getErrorTable() {
    return this.errorTable;
  }

  public String setErrorTable( String val ) {
    return this.errorTable = val;
  }

  public String getErrorTable2() {
    return this.errorTable2;
  }

  public String setErrorTable2( String val ) {
    return this.errorTable2 = val;
  }

  public String getAccessLogFile() {
    return this.accessLogFile;
  }

  public String setAccessLogFile( String val ) {
    return this.accessLogFile = val;
  }

  public String getUpdateLogFile() {
    return this.updateLogFile;
  }

  public String setUpdateLogFile( String val ) {
    return this.updateLogFile = val;
  }

  public int getActionType() {
    return this.actionType;
  }

  public int setActionType( int val ) {
    return this.actionType = val;
  }

  public void setDatabaseMeta( DatabaseMeta database ) {
    this.databaseMeta = database;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName( String tableName ) {
    this.tableName = tableName;
  }

  public String[] getFieldTable() {
    return fieldTable;
  }

  public void setFieldTable( String[] fieldTable ) {
    this.fieldTable = fieldTable;
  }

  public String[] getFieldStream() {
    return fieldStream;
  }

  public void setFieldStream( String[] fieldStream ) {
    this.fieldStream = fieldStream;
  }

  public Boolean[] getFieldUpdate() {
    return fieldUpdate;
  }

  public void setFieldUpdate( Boolean[] fieldUpdate ) {
    this.fieldUpdate = fieldUpdate;
  }

  public String[] getKeyStream() {
    return keyStream;
  }

  public void setKeyStream( String[] fieldStream ) {
    this.keyStream = fieldStream;
  }

  public String[] getKeyLookup() {
    return keyLookup;
  }

  public void setKeyLookup( String[] fieldStream ) {
    this.keyLookup = fieldStream;
  }

  public String[] getKeyCondition() {
    return keyCondition;
  }

  public void setKeyCondition( String[] fieldStream ) {
    this.keyCondition = fieldStream;
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> arg2 )
    throws KettleXMLException {
    readData( stepnode, databases );
  }

  public void allocate( int nrkeys, int nrvalues ) {
    keyStream = new String[nrkeys];
    keyLookup = new String[nrkeys];
    keyCondition = new String[nrkeys];
    fieldTable = new String[nrvalues];
    fieldStream = new String[nrvalues];
    fieldUpdate = new Boolean[nrvalues];
  }

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

  public void setDefault() {
    fieldTable = null;
    databaseMeta = null;
    allocate( 0, 0 );
  }

  public String getXML() {
    StringBuffer retval = new StringBuffer( 3000 );

    // common
    retval
        .append( "    " ).append( XMLHandler.addTagValue( "connection", databaseMeta == null ? "" : databaseMeta.getName() ) ); //$NON-NLS-3$
    if ( generateScript != null ) {
      retval.append( "    " ).append( XMLHandler.addTagValue( "generateScript", generateScript ) );
    }
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
    retval.append( "    " ).append( XMLHandler.addTagValue( "scriptFileName", scriptFileName ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "actionType", actionType ) );

    // pre-existing options
    retval.append( "    " ).append( XMLHandler.addTagValue( "existingScriptFile", existingScriptFile ) );
    if ( substituteScriptFile != null ) {
      retval.append( "    " ).append( XMLHandler.addTagValue( "substituteScriptFile", substituteScriptFile ) );
    }
    retval.append( "    " ).append( XMLHandler.addTagValue( "existingVariableFile", existingVariableFile ) );
    if ( substituteVariableFile != null ) {
      retval.append( "    " ).append( XMLHandler.addTagValue( "substituteVariableFile", substituteVariableFile ) );
    }

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

  @Override
  public void readRep( Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String, Counter> arg3 )
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
      // connection = rep.getStepAttributeString(id_step, "connection");
      tbuildPath = rep.getStepAttributeString( id_step, "tbuildPath" );
      tbuildLibPath = rep.getStepAttributeString( id_step, "tbuildLibPath" );
      libPath = rep.getStepAttributeString( id_step, "libPath" );
      tdicuLibPath = rep.getStepAttributeString( id_step, "tdicuLibPath" );
      copLibPath = rep.getStepAttributeString( id_step, "copLibPath" );
      twbRoot = rep.getStepAttributeString( id_step, "twbRoot" );
      installPath = rep.getStepAttributeString( id_step, "installPath" );
      jobName = rep.getStepAttributeString( id_step, "jobName" );
      fifoFileName = rep.getStepAttributeString( id_step, "fifoFileName" );

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

  @Override
  public void saveRep( Repository rep, ObjectId id_transformation, ObjectId id_step ) throws KettleException {

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

      rep.saveStepAttribute( id_transformation, id_step, "schema", schemaName );
      rep.saveStepAttribute( id_transformation, id_step, "table", tableName );
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
            throw new KettleStepException( "Unable to find field [" + fieldStream[i] + "] in the input rows" );
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

  /*
   * public void analyseImpact(List<DatabaseImpact> impact, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface
   * prev, String input[], String output[], RowMetaInterface info, Repository repository, IMetaStore metaStore) throws
   * KettleStepException { if (prev != null) { // Insert dateMask fields : read/write for (int i = 0; i <
   * fieldTable.length; i++) { ValueMetaInterface v = prev.searchValueMeta(fieldStream[i]);
   * 
   * DatabaseImpact ii = new DatabaseImpact(DatabaseImpact.TYPE_IMPACT_READ_WRITE, transMeta.getName(),
   * stepMeta.getName(), databaseMeta .getDatabaseName(), transMeta.environmentSubstitute(tableName), fieldTable[i],
   * fieldStream[i], v!=null?v.getOrigin():"?", "", "Type = " + v.toStringMeta()); //$NON-NLS-3$ impact.add(ii); } } }
   */

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
      Trans trans ) {
    myStep = new TeraDataBulkLoader( stepMeta, stepDataInterface, cnr, transMeta, trans );
    return myStep;
  }

  public StepDataInterface getStepData() {
    return new TeraDataBulkLoaderData();
  }

  public DatabaseMeta[] getUsedDatabaseConnections() {
    if ( databaseMeta != null ) {
      return new DatabaseMeta[] { databaseMeta };
    } else {
      return super.getUsedDatabaseConnections();
    }
  }

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
            throw new KettleException(
              BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.Exception.TableNotFound" ) );
          }
        } else {
          throw new KettleException(
              BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.Exception.TableNotSpecified" ) );
        }
      } catch ( Exception e ) {
        throw new KettleException(
            BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.Exception.ErrorGettingFields" ), e );
      } finally {
        db.disconnect();
      }
    } else {
      throw new KettleException(
          BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.Exception.ConnectionNotDefined" ) );
    }

  }

  /**
   * @return the schemaName
   */

  public static String[] getFieldFormatTypeCodes() {
    return fieldFormatTypeCodes;
  }

  public static String[] getFieldFormatTypeDescriptions() {
    return fieldFormatTypeDescriptions;
  }

  public static String getFieldFormatTypeCode( int type ) {
    return fieldFormatTypeCodes[type];
  }

  public static String getFieldFormatTypeDescription( int type ) {
    return fieldFormatTypeDescriptions[type];
  }

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

  @Override
  public String getMissingDatabaseConnectionInformationMessage() {
    // TODO Auto-generated method stub
    return null;
  }

}
