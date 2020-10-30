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

import java.io.DataInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;

/**
 * The Class TeraDataBulkLoaderRoutines.
 */
public class TeraDataBulkLoaderRoutines {

  /** The pkg. */
  private static Class<?> PKG = TeraDataBulkLoaderMeta.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$

  /** The parent. */
  private TeraDataBulkLoader parent;

  /** The meta. */
  private TeraDataBulkLoaderMeta meta;

  /** The script file. */
  private OutputStream scriptFile;

  /** The script file print stream. */
  private PrintStream scriptFilePrintStream;

  /** The Constant SIZEOF_INT. */
  static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;

  /**
   * Instantiates a new tera data bulk loader routines.
   *
   * @param parent the parent
   * @param meta the meta
   */
  public TeraDataBulkLoaderRoutines( TeraDataBulkLoader parent, TeraDataBulkLoaderMeta meta ) {
    this.parent = parent;
    this.meta = meta;
  }

  @VisibleForTesting
  String getTargetSchema( boolean isPreview ) {

    if ( this.meta.getSchemaName() == null || this.meta.getSchemaName().isEmpty() ) {
      return isPreview ? this.meta.getDbName() : parent.environmentSubstitute( this.meta.getDbName() );
    }
    return isPreview ? this.meta.getSchemaName() : parent.environmentSubstitute( this.meta.getSchemaName() );
  }
  /**
   * Creates the insert command.
   *
   * @return the string
   */
  private String createInsertCommand( boolean isPreview ) {
    StringBuffer cmd = new StringBuffer();
    cmd.append( " INSERT INTO " + getTargetSchema( isPreview ) + '.' + ( isPreview ? this.meta.getTableName() : parent.environmentSubstitute( this.meta.getTableName() + "\n" ) ) );
    cmd.append( "   (\n" );
    String[] fieldTable = this.meta.getFieldTable();
    for ( int i = 0; i < fieldTable.length; i++ ) {
      cmd.append( "       " + fieldTable[i] );
      if ( i < fieldTable.length - 1 ) {
        cmd.append( "," );
      }
      cmd.append( "\n" );
    }
    cmd.append( "   )\n" );
    cmd.append( " VALUES\n" );
    cmd.append( "   (\n" );
    String[] fieldStream = this.meta.getFieldStream();
    for ( int i = 0; i < fieldStream.length; i++ ) {
      cmd.append( "       :" + fieldStream[i] );
      if ( i < fieldStream.length - 1 ) {
        cmd.append( "," );
      }
      cmd.append( "\n" );
    }
    cmd.append( "   )\n" );

    return cmd.toString();
  }

  /**
   * Creates the upsert command.
   *
   * @return the string
   */
  @VisibleForTesting
  String createUpsertCommand( boolean isPreview ) {
    StringBuffer updatecmd = new StringBuffer();
    StringBuffer insertcmd = new StringBuffer();

    updatecmd.append( " UPDATE " + getTargetSchema( isPreview ) + '.' + ( isPreview ? this.meta.getTableName() : parent.environmentSubstitute( this.meta.getTableName() ) ) + " SET\n" );
    String[] fieldTable = this.meta.getFieldTable();
    String[] fieldStream = this.meta.getFieldStream();
    Boolean[] fieldUpdate = this.meta.getFieldUpdate();

    // Do the where clause first so that any where columns can be filtered out of the update
    Map<String, Boolean> usedAsKey = new HashMap<String, Boolean>();
    StringBuffer whereClause = new StringBuffer();
    for ( int i = 0; i < this.meta.getKeyStream().length; i++ ) {
      whereClause.append( "        " + ( i == 0 ? "WHERE " : "AND" ) + " " );
      whereClause.append( this.meta.getKeyLookup()[i] + " " + this.meta.getKeyCondition()[i] + " :"
          + this.meta.getKeyStream()[i] + "\n" );
      usedAsKey.put( this.meta.getKeyLookup()[i], new Boolean( true ) );
    }

    // Build the update
    int numberOfFieldsAdded = 0;
    for ( int i = 0; i < fieldTable.length; i++ ) {
      // cant include this - causes teradata error if used in WHERE, or user simply selects N for update
      if ( !( usedAsKey.containsKey( fieldTable[i] ) || !fieldUpdate[i] ) ) {
        if ( numberOfFieldsAdded > 0 ) {
          updatecmd.append( "," );
        }
        updatecmd.append( "          " + fieldTable[i] + " = :" + fieldStream[i] );
        updatecmd.append( "\n" );
        numberOfFieldsAdded++;
      }
    }
    updatecmd.append( whereClause + ";\n" );

    insertcmd.append( createInsertCommand( isPreview ) );

    return quote( updatecmd.toString() ) + ",\n" + quote( insertcmd.toString() );
  }

  /**
   * Creates the step.
   *
   * @param label the label
   * @param code the code
   * @param operator the operator
   * @return the string
   */
  private String createStep( String label, String code, String operator ) {
    StringBuffer cmd = new StringBuffer( "STEP " + label + "(\n" + code + "\n" );
    if ( operator != null ) {
      cmd.append( operator );
    }
    cmd.append( "\n);\n" );
    return cmd.toString();
  }

  /**
   * To operator.
   *
   * @param operator the operator
   * @return the string
   */
  private String toOperator( String operator ) {
    return "TO OPERATOR ( " + operator + " )";
  }

  /**
   * Select operator.
   *
   * @param operator the operator
   * @return the string
   */
  private String selectOperator( String operator ) {
    return "SELECT * FROM OPERATOR ( " + operator + " )";
  }

  /**
   * To select operator.
   *
   * @param toOp the to op
   * @param selOp the sel op
   * @return the string
   */
  private String toSelectOperator( String toOp, String selOp ) {
    StringBuffer cmd = new StringBuffer( toOperator( toOp ) + "\n" );
    if ( selOp != null ) {
      cmd.append( selectOperator( selOp ) );
    }
    cmd.append( ";\n" );
    return cmd.toString();
  }

  /**
   * Quote.
   *
   * @param s the s
   * @return the string
   */
  private String quote( String s ) {
    return "'" + s + "'";
  }

  /**
   * Paren.
   *
   * @param s the s
   * @return the string
   */
  private String paren( String s ) {
    return "(" + s + ")";
  }

  /**
   * The Class ApplyClause.
   */
  private class ApplyClause {

    /** The field list. */
    private List<String> fieldList = new ArrayList<String>();

    /**
     * Instantiates a new apply clause.
     */
    ApplyClause() {
    }

    /**
     * Adds the clause.
     *
     * @param c the c
     */
    private void addClause( String c ) {
      if ( !Const.isEmpty( c ) ) {
        fieldList.add( c );
      }
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      StringBuffer cmd = new StringBuffer( "APPLY \n" );
      for ( int i = 0; i < fieldList.size(); i++ ) {
        cmd.append( "     " + paren( fieldList.get( i ) ) );
        if ( i < fieldList.size() - 1 ) {
          cmd.append( "," );
        }
        cmd.append( "\n" );
      }
      return cmd.toString();
    }
  }

  /**
   * The Class DefineSchema.
   */
  @VisibleForTesting
  class DefineSchema {

    /** The Constant TYPE_SCHEMA. */
    static final int TYPE_SCHEMA = 1;

    /** The Constant TYPE_OPERATOR. */
    static final int TYPE_OPERATOR = 2;

    /** The type. */
    private int type = 0;

    /** The name. */
    private String name;

    /** The field list. */
    private List<String> fieldList = new ArrayList<String>();

    /** The type name. */
    private String typeName;

    /** The schema name. */
    private String schemaName;

    /**
     * Instantiates a new define schema.
     *
     * @param operatorName the operator name
     * @param typeName the type name
     * @param schemaName the schema name
     */
    DefineSchema( String operatorName, String typeName, String schemaName ) {
      this.name = operatorName;
      this.typeName = typeName;
      this.schemaName = schemaName;
      this.type = TYPE_OPERATOR; /* operator */

    }

    /**
     * Instantiates a new define schema.
     *
     * @param tableName the table name
     */
    DefineSchema( String tableName ) {
      this.name = tableName;
      this.type = TYPE_SCHEMA; /* table schema */
    }

    /**
     * Adds the field.
     *
     * @param name the name
     * @param value the value
     */
    private void addField( String name, String value ) {
      StringBuffer item = new StringBuffer( "VARCHAR " + name );
      if ( !( value == null || value.equals( "" ) ) ) {
        item.append( " = '" + value + "'" );
        fieldList.add( item.toString() );
      }
    }

    /**
     * Adds the field.
     *
     * @param name the name
     * @param valueType the value type
     * @param len the len
     * @throws KettleException the kettle exception
     */
    private void addField( String name, int valueType, int len ) throws KettleException {
      String type = null;
      switch ( valueType ) {
        case ValueMetaInterface.TYPE_STRING:
          type = "VARCHAR(" + len + ")";
          break;
        case ValueMetaInterface.TYPE_INTEGER:
          type = "BIGINT";
          break;
        case ValueMetaInterface.TYPE_BIGNUMBER:
          type = "FLOAT";
          break;
        case ValueMetaInterface.TYPE_DATE:
          type = "TIMESTAMP(6)";
          break;
        case ValueMetaInterface.TYPE_NUMBER:
          type = "FLOAT";
          break;
        case ValueMetaInterface.TYPE_BOOLEAN:
          type = "BYTEINT";
          break;
        default:
          throw new KettleException( BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.Exception.UnhandledType" ) );
      }
      fieldList.add( name + "     " + type );
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      StringBuffer cmd = new StringBuffer();
      switch ( this.type ) {
        case TYPE_SCHEMA:
          cmd.append( "DEFINE SCHEMA " + this.name );

          break;
        case TYPE_OPERATOR:
          cmd.append( "DEFINE OPERATOR " + this.name + "\nTYPE " + this.typeName
              + ( schemaName != null ? ( "\nSCHEMA " + this.schemaName ) : "" ) + "\nATTRIBUTES" );
          break;
      }
      cmd.append( "\n  (\n" );
      for ( int i = 0; i < fieldList.size(); i++ ) {
        cmd.append( "     " + fieldList.get( i ) );
        if ( i < fieldList.size() - 1 ) {
          cmd.append( "," );
        }
        cmd.append( "\n" );
      }
      cmd.append( "  );\n" );
      return cmd.toString();
    }
  }

  // currently not used
  /**
   * Adds the missing options.
   *
   * @return the string
   */
  private String addMissingOptions() {
    StringBuffer cmd = new StringBuffer();
    if ( this.meta.getIgnoreDupUpdate() ) {
      cmd.append( "IGNORE DUPLICATE UPDATE ROWS\n" );
    }
    if ( this.meta.getInsertMissingUpdate() ) {
      cmd.append( "INSERT FOR MISSING UPDATE ROWS\n" );
    }
    if ( this.meta.getIgnoreMissingUpdate() ) {
      cmd.append( "IGNORE MISSING UPDATE ROWS\n" );
    }
    return cmd.toString();
  }

  /**
   * Teradata job.
   *
   * @param jobName the job name
   * @param description the description
   * @param code the code
   * @return the string
   */
  private String teradataJob( String jobName, String description, String code ) {
    return "DEFINE JOB " + jobName + "\nDESCRIPTION '" + description + "'\n(\n" + code + ");";
  }

  /**
   * Drop table.
   *
   * @param user the user
   * @param suffix the suffix
   * @return the string
   */
  public String dropTable( boolean isPreview, String user, String suffix ) {
    return "DROP TABLE " + ( Const.isEmpty( user ) ? ( isPreview ? this.meta.getTableName() : parent.environmentSubstitute( this.meta.getTableName() ) ) + "_" + suffix : user );
  }

  /**
   * Creates the script file.
   *
   * @return the string
   * @throws Exception the exception
   */
  public String createScriptFile() throws Exception {
    File tempScriptFile;

    if ( meta.getGenerateScript() ) {
      tempScriptFile = File.createTempFile( FilenameUtils.getBaseName( parent.environmentSubstitute( meta.getScriptFileName() ) ), "" );
    } else {
      tempScriptFile = File.createTempFile( FilenameUtils.getBaseName( parent.environmentSubstitute( meta.getExistingScriptFile() ) ), "" );
    }
    tempScriptFile.deleteOnExit();

    try {
      scriptFile = FileUtils.openOutputStream( tempScriptFile );
      scriptFilePrintStream = new PrintStream( scriptFile );
    } catch ( IOException e ) {
      throw new KettleException(
          BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.Exception.OpenScriptFile", scriptFile ), e );
    }

    if ( meta.getGenerateScript() ) {
      createGeneratedScriptFile();
    } else {
      createFromExistingScriptFile();
    }
    scriptFilePrintStream.close();
    IOUtils.closeQuietly( scriptFile );
    return tempScriptFile.getAbsolutePath();
  }

  /**
   * Creates the from existing script file.
   *
   * @throws Exception the exception
   */
  public void createFromExistingScriptFile() throws Exception {
    FileInputStream originalScript = new FileInputStream( parent.environmentSubstitute( this.meta.getExistingScriptFile() ) );
    DataInputStream in = new DataInputStream( originalScript );
    BufferedReader br = new BufferedReader( new InputStreamReader( in ) );
    String strLine;
    while ( ( strLine = br.readLine() ) != null ) {
      if ( this.meta.getSubstituteControlFile() ) {
        scriptFilePrintStream.print( parent.environmentSubstitute( strLine ) + "\n" );
      } else {
        scriptFilePrintStream.print( strLine + "\n" );
      }
    }
    // Close the input stream
    in.close();
  }

  /**
   * Creates the generated script file.
   *
   * @return the string
   * @throws Exception the exception
   */
  public String createGeneratedScriptFile() throws Exception {
    // this is the method called when generating the actual script
    // to execute. we will construct the maps for types and length
    // here and pass them. Fix for ordering issue found 11/11/13
    // in which the fields in the stream were ordered differently
    // than in the field table, leading to incorrect typing.
    Map<String, Integer> inputFieldTypes = new HashMap<String, Integer>();
    Map<String, Integer> inputFieldLength = new HashMap<String, Integer>();

    for ( int i = 0; i < parent.getInputRowMeta().size(); i++ ) {
      inputFieldTypes.put( parent.getInputRowMeta().getValueMeta( i ).getName(), parent.getInputRowMeta().getValueMeta(
          i ).getType() );
      inputFieldLength.put( parent.getInputRowMeta().getValueMeta( i ).getName(), parent.getInputRowMeta()
          .getValueMeta( i ).getLength() );
    }
    return createGeneratedScriptFile( inputFieldTypes, inputFieldLength );
  }

  /**
   * Creates the generated script file.
   *
   * @param inputFieldTypes the input field types
   * @param inputFieldLength the input field length
   * @return the string
   * @throws Exception the exception
   */
  public String createGeneratedScriptFile( Map<String, Integer> inputFieldTypes, Map<String, Integer> inputFieldLength )
    throws Exception {
    // Schema info
    boolean isPreview = parent == null;
    String hiddenPassword = BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.HiddenPassword" );

    DefineSchema tableSchema = new DefineSchema( isPreview ? this.meta.getSchemaName() : parent.environmentSubstitute( this.meta.getSchemaName() ) );
    // Iterate over the FieldStream array, add each with its types
    String[] fieldStream = this.meta.getFieldStream();
    if ( inputFieldTypes.size() == 0 || fieldStream == null || inputFieldTypes == null || inputFieldLength == null ) {
      return ( BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.NoInputStream" ) );
    }
    System.out.println( "fieldStream length is " + fieldStream.length );
    for ( int i = 0; i < fieldStream.length; i++ ) {
      int len;
      int type;
      type = inputFieldTypes.get( fieldStream[i] );
      len = inputFieldLength.get( fieldStream[i] );
      tableSchema.addField( fieldStream[i], type, len );
    }

    DefineSchema dataConnector = getDataConnector( isPreview );
    // DDL Operator
    DefineSchema ddlOptions = getDdlOptions( isPreview, hiddenPassword );
    // Update Operator
    DefineSchema updateOptions = getUpdateOptions( isPreview, hiddenPassword );

    // Drop tables as needed......
    String dropTables = getDropTables( isPreview );

    // Specific Command
    String loadCommand = null;
    ApplyClause cmdApply = new ApplyClause();
    switch ( this.meta.getActionType() ) {
      case 0:
        cmdApply.addClause( quote( createInsertCommand( isPreview ) ) );
        loadCommand =
            createStep( "Load_Table", cmdApply.toString(), toSelectOperator( "UPDATE_OPERATOR[2]",
                "ACCESS_MODULE_READER[2]" ) );
        break;
      case 1:
        cmdApply.addClause( createUpsertCommand( isPreview ) );
        loadCommand =
            createStep( "Upsert_Table", cmdApply.toString() + addMissingOptions(), toSelectOperator(
                "UPDATE_OPERATOR[2]", "ACCESS_MODULE_READER[2]" ) );
        break;
    }

    String script =
        teradataJob( BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.Script.Name" ), // "JobName_Goes_Here",
            BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.Script.Description" ), // "Description goes here",
            ( tableSchema != null ? tableSchema.toString() : "" ) + ( ddlOptions != null ? ddlOptions.toString() : "" )
                + ( dataConnector != null ? dataConnector.toString() : "" )
                + ( updateOptions != null ? updateOptions.toString() : "" ) + ( dropTables != null ? dropTables : "" )
                + loadCommand );

    if ( scriptFilePrintStream != null ) {
      scriptFilePrintStream.print( script );
    }
    return script;
  }

  @VisibleForTesting
  String getDropTables( boolean isPreview ) {
    String dropTables = null;
    boolean drop = false;
    ApplyClause dropClauses = new ApplyClause();
    if ( this.meta.getDropLogTable() ) {
      dropClauses.addClause( quote( dropTable( isPreview, isPreview ? this.meta.getLogTable() : parent.environmentSubstitute( this.meta.getLogTable() ), "" ) ) );
      drop = true;
    }
    if ( this.meta.getDropWorkTable() ) {
      dropClauses.addClause( quote( dropTable( isPreview, isPreview ? this.meta.getWorkTable() : parent.environmentSubstitute( this.meta.getWorkTable() ), "WT" ) ) );
      drop = true;
    }
    if ( this.meta.getDropErrorTable() ) {
      dropClauses.addClause( quote( dropTable( isPreview, isPreview ? this.meta.getErrorTable() : parent.environmentSubstitute( this.meta.getErrorTable() ), "ET" ) ) );
      drop = true;
    }
    if ( this.meta.getDropErrorTable2() ) {
      dropClauses.addClause( quote( dropTable( isPreview, isPreview ? this.meta.getErrorTable2() : parent.environmentSubstitute( this.meta.getErrorTable2() ), "UV" ) ) );
      drop = true;
    }
    if ( drop ) {
      dropTables = createStep( "Setup_tables", dropClauses.toString(), toSelectOperator( "DDL_OPERATOR", null ) );
    }
    return dropTables;
  }

  @VisibleForTesting
  DefineSchema getDataConnector( boolean isPreview ) {
    DefineSchema dataConnector =
        new DefineSchema( "ACCESS_MODULE_READER", "DATACONNECTOR PRODUCER", isPreview ? this.meta.getSchemaName() : parent.environmentSubstitute( this.meta.getSchemaName() ) );
    dataConnector.addField( "PrivateLogName", isPreview ? this.meta.getAccessLogFile() : parent.environmentSubstitute( this.meta.getAccessLogFile() ) );
    dataConnector.addField( "AccessModuleName", "np_axsmod.so" );
    dataConnector.addField( "AccessModuleInitStr", null );
    dataConnector.addField( "FileName", parent != null ? parent.data.fifoFilename : this.meta.getFifoFileName() );
    dataConnector.addField( "Format", "Unformatted" );
    dataConnector.addField( "OpenMode", "Read" );
    return dataConnector;
  }

  @VisibleForTesting
  DefineSchema getDdlOptions( boolean isPreview, String hiddenPassword ) {
    DefineSchema ddlOptions = new DefineSchema( "DDL_OPERATOR", "DDL", null );
    ddlOptions.addField( "TdpId             ", this.meta.getDatabaseMeta().getHostname() );
    ddlOptions.addField( "UserName          ", this.meta.getDatabaseMeta().getUsername() );
    ddlOptions.addField( "UserPassword      ", isPreview ? hiddenPassword : this.meta.getDatabaseMeta().getPassword() );
    ddlOptions.addField( "ErrorList         ", "3807" );
    return ddlOptions;
  }

  @VisibleForTesting
  DefineSchema getUpdateOptions( boolean isPreview, String hiddenPassword ) {
    DefineSchema updateOptions = new DefineSchema( "UPDATE_OPERATOR", "UPDATE", "*" );
    updateOptions.addField( "TdpId             ", this.meta.getDatabaseMeta().getHostname() );
    updateOptions.addField( "PrivateLogName    ", isPreview ? this.meta.getUpdateLogFile() : parent.environmentSubstitute( this.meta.getUpdateLogFile() ) );
    updateOptions.addField( "UserName          ", this.meta.getDatabaseMeta().getUsername() );
    updateOptions.addField( "UserPassword      ", isPreview ? hiddenPassword : this.meta.getDatabaseMeta()
      .getPassword() );
    updateOptions.addField( "LogTable          ", isPreview ? this.meta.getLogTable() : parent.environmentSubstitute( this.meta.getLogTable() ) );
    updateOptions.addField( "TargetTable       ", getTargetSchema( isPreview ) + "." + ( isPreview ? this.meta.getTableName() : parent.environmentSubstitute( this.meta.getTableName() ) ) );
    updateOptions.addField( "WorkTable         ", isPreview ? this.meta.getWorkTable() : parent.environmentSubstitute( this.meta.getWorkTable() ) );
    updateOptions.addField( "ErrorTable1       ", isPreview ? this.meta.getErrorTable() : parent.environmentSubstitute( this.meta.getErrorTable() ) );
    updateOptions.addField( "ErrorTable2       ", isPreview ? this.meta.getErrorTable2() : parent.environmentSubstitute( this.meta.getErrorTable2() ) );
    return updateOptions;
  }

  /**
   * Resolve file name.
   *
   * @param fileName          the filename to resolve. may contain Kettle Environment variables.
   * @return the data file name.
   * @throws KettleException the kettle exception
   */
  @SuppressWarnings( "unused" )
  private String resolveFileName( final String fileName ) throws KettleException {
    final FileObject fileObject = KettleVFS.getFileObject( parent.environmentSubstitute( fileName ) );
    return KettleVFS.getFilename( fileObject );
  }

  /**
   * ***************************************
   * formatting routines for "unformatted" output
   * ****************************************.
   *
   * @param valueMeta the value meta
   * @param valueData the value data
   * @return the byte[]
   * @throws KettleValueException the kettle value exception
   */

  static byte[] convertChar( ValueMetaInterface valueMeta, Object valueData ) throws KettleValueException {
    String string = valueMeta.getString( valueData );
    if ( string != null ) {
      return ( string.getBytes() );
    }
    return null;
  }

  /**
   * Convert varchar.
   *
   * @param string the string
   * @return the byte[]
   */
  static byte[] convertVarchar( String string ) {
    ByteBuffer b = ByteBuffer.allocate( 4 ).order( ByteOrder.LITTLE_ENDIAN );
    short strlen = 0;

    if ( string != null ) {
      strlen = (short) string.length();
      b.putShort( strlen );
    } else {
      b = ByteBuffer.allocate( 2 ).order( ByteOrder.LITTLE_ENDIAN );
      b.putShort( (short) 0 );
      return b.array();
    }
    byte[] result = new byte[2 + strlen];
    System.arraycopy( b.array(), 0, result, 0, 2 );
    System.arraycopy( string.getBytes(), 0, result, 2, strlen );
    return result;
  }

  /**
   * Convert long.
   *
   * @param integer the integer
   * @return the byte[]
   */
  static byte[] convertLong( Long integer ) {
    ByteBuffer b = ByteBuffer.allocate( 8 ).order( ByteOrder.LITTLE_ENDIAN );
    b.putLong( integer != null ? integer : 0 );
    return b.array();
  }

  /**
   * Convert float.
   *
   * @param d the d
   * @return the byte[]
   */
  static byte[] convertFloat( Double d ) {
    ByteBuffer b = ByteBuffer.allocate( 8 ).order( ByteOrder.LITTLE_ENDIAN );
    b.putDouble( d != null ? d : 0 );
    return b.array();
  }

  /**
   * Convert bignum.
   *
   * @param d the d
   * @return the byte[]
   */
  static byte[] convertBignum( BigDecimal d ) {
    if ( d != null ) {
      return convertFloat( d.doubleValue() );
    } else {
      return convertFloat( new Double( 0 ) );
    }
  }

  /**
   * Convert date time.
   *
   * @param ts the ts
   * @return the byte[]
   */
  static byte[] convertDateTime( Date ts ) {
    if ( ts != null ) {
      SimpleDateFormat fmt = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSSSSS" );
      String fmtdate = fmt.format( ts );
      return fmtdate.getBytes();
    }
    byte[] result = new byte[26];
    System.arraycopy( "0001-01-01 00:00:00.000000".getBytes(), 0, result, 0, 26 );
    return result;
  }

  /**
   * Convert boolean.
   *
   * @param val the val
   * @return the byte[]
   */
  static byte[] convertBoolean( Boolean val ) {
    byte[] b = new byte[1];
    if ( val != null && val ) {
      b[0] = 1;
    } else {
      b[0] = 0;
    }
    return b;
  }

}
