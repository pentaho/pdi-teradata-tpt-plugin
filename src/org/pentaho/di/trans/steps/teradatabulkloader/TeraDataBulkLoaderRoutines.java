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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs.FileObject;
import org.mvel2.util.StringAppender;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;

public class TeraDataBulkLoaderRoutines {
  private static Class<?> PKG = TeraDataBulkLoaderMeta.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$
  private TeraDataBulkLoader parent;
  private TeraDataBulkLoaderMeta meta;
  private OutputStream scriptFile;
  private PrintStream scriptFilePrintStream;
  static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;

  public TeraDataBulkLoaderRoutines( TeraDataBulkLoader parent, TeraDataBulkLoaderMeta meta ) {
    this.parent = parent;
    this.meta = meta;
  }

  private String createInsertCommand() {
    StringAppender cmd = new StringAppender();
    cmd.append( " INSERT INTO " + this.meta.getDbName() + '.' + this.meta.getTableName() + "\n" );
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

  private String createUpsertCommand() {
    StringAppender updatecmd = new StringAppender();
    StringAppender insertcmd = new StringAppender();

    updatecmd.append( " UPDATE " + this.meta.getDbName() + '.' + this.meta.getTableName() + " SET\n" );
    String[] fieldTable = this.meta.getFieldTable();
    String[] fieldStream = this.meta.getFieldStream();
    Boolean[] fieldUpdate = this.meta.getFieldUpdate();

    // Do the where clause first so that any where columns can be filtered out of the update
    Map<String, Boolean> usedAsKey = new HashMap<String, Boolean>();
    StringAppender whereClause = new StringAppender();
    for ( int i = 0; i < this.meta.getKeyStream().length; i++ ) {
      whereClause.append( "        " + ( i == 0 ? "WHERE " : "AND" ) + " " );
      whereClause.append( this.meta.getKeyLookup()[i] + " " + this.meta.getKeyCondition()[i] + " :"
          + this.meta.getKeyStream()[i] + "\n" );
      usedAsKey.put( this.meta.getKeyLookup()[i], new Boolean( true ) );
    }

    // Build the update
    for ( int i = 0; i < fieldTable.length; i++ ) {
      // cant include this - causes teradata error if used in WHERE, or user simply selects N for update
      if ( !( usedAsKey.containsKey( fieldTable[i] ) || !fieldUpdate[i] ) ) {
        if ( i > 0 ) {
          updatecmd.append( "," );
        }
        updatecmd.append( "          " + fieldTable[i] + " = :" + fieldStream[i] );
        updatecmd.append( "\n" );
      }
    }
    updatecmd.append( whereClause + ";\n" );

    insertcmd.append( createInsertCommand() );

    return quote( updatecmd.toString() ) + ",\n" + quote( insertcmd.toString() );
  }

  private String createStep( String label, String code, String operator ) {
    StringAppender cmd = new StringAppender( "STEP " + label + "(\n" + code + "\n" );
    if ( operator != null ) {
      cmd.append( operator );
    }
    cmd.append( "\n);\n" );
    return cmd.toString();
  }

  private String toOperator( String operator ) {
    return "TO OPERATOR ( " + operator + " )";
  }

  private String selectOperator( String operator ) {
    return "SELECT * FROM OPERATOR ( " + operator + " )";
  }

  private String toSelectOperator( String toOp, String selOp ) {
    StringAppender cmd = new StringAppender( toOperator( toOp ) + "\n" );
    if ( selOp != null ) {
      cmd.append( selectOperator( selOp ) );
    }
    cmd.append( ";\n" );
    return cmd.toString();
  }

  private String quote( String s ) {
    return "'" + s + "'";
  }

  private String paren( String s ) {
    return "(" + s + ")";
  }

  private class ApplyClause {
    private List<String> fieldList = new ArrayList<String>();

    ApplyClause() {
    }

    private void addClause( String c ) {
      if ( !Const.isEmpty( c ) ) {
        fieldList.add( c );
      }
    }

    public String toString() {
      StringAppender cmd = new StringAppender( "APPLY \n" );
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

  private class DefineSchema {
    static final int TYPE_SCHEMA = 1;
    static final int TYPE_OPERATOR = 2;
    private int type = 0;
    private String name;
    private List<String> fieldList = new ArrayList<String>();
    private String typeName;
    private String schemaName;

    DefineSchema( String operatorName, String typeName, String schemaName ) {
      this.name = operatorName;
      this.typeName = typeName;
      this.schemaName = schemaName;
      this.type = TYPE_OPERATOR; /* operator */

    }

    DefineSchema( String tableName ) {
      this.name = tableName;
      this.type = TYPE_SCHEMA; /* table schema */
    }

    private void addField( String name, String value ) {
      StringAppender item = new StringAppender( "VARCHAR " + name );
      if ( !( value == null || value.equals( "" ) ) ) {
        item.append( " = '" + value + "'" );
        fieldList.add( item.toString() );
      }
    }

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
          throw new KettleException( "Unhandled type in stream" );
      }
      fieldList.add( name + "     " + type );
    }

    public String toString() {
      StringAppender cmd = new StringAppender();
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
  private String addMissingOptions() {
    StringAppender cmd = new StringAppender();
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

  private String teradataJob( String jobName, String description, String code ) {
    return "DEFINE JOB " + jobName + "\nDESCRIPTION '" + description + "'\n(\n" + code + ");";
  }

  public String dropTable( String user, String suffix ) {
    return "DROP TABLE " + ( Const.isEmpty( user ) ? this.meta.getTableName() + "_" + suffix : user );
  }

  public String createScriptFile() throws Exception {
    File tempScriptFile;

    if ( this.meta.getGenerateScript() ) {
      tempScriptFile = File.createTempFile( FilenameUtils.getBaseName( this.meta.getScriptFileName() ), "" );
    } else {
      tempScriptFile = File.createTempFile( FilenameUtils.getBaseName( this.meta.getExistingScriptFile() ), "" );
    }
    tempScriptFile.deleteOnExit();

    try {
      this.scriptFile = FileUtils.openOutputStream( tempScriptFile );
      this.scriptFilePrintStream = new PrintStream( scriptFile );
    } catch ( IOException e ) {
      throw new KettleException( "Cannot open script file [path=" + this.scriptFile + "]", e );
    }

    if ( this.meta.getGenerateScript() ) {
      createGeneratedScriptFile();
    } else {
      createFromExistingScriptFile();
    }
    this.scriptFilePrintStream.close();
    IOUtils.closeQuietly( this.scriptFile );
    return tempScriptFile.getAbsolutePath();
  }

  public void createFromExistingScriptFile() throws Exception {
    FileInputStream originalScript = new FileInputStream( this.meta.getExistingScriptFile() );
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

  public String createGeneratedScriptFile( Map<String, Integer> inputFieldTypes, Map<String, Integer> inputFieldLength )
    throws Exception {
    // Schema info
    boolean isPreview = parent == null;
    String hiddenPassword = "*** password hidden in preview ***";

    DefineSchema tableSchema = new DefineSchema( this.meta.getSchemaName() );
    // Iterate over the FieldStream array, add each with its types
    String[] fieldStream = this.meta.getFieldStream();
    if ( inputFieldTypes.size() == 0 || fieldStream == null || inputFieldTypes == null || inputFieldLength == null ) {
      return ( "No input stream - disabled hop?" );
    }
    System.out.println( "fieldStream length is " + fieldStream.length );
    for ( int i = 0; i < fieldStream.length; i++ ) {
      int len;
      int type;
      type = inputFieldTypes.get( fieldStream[i] );
      len = inputFieldLength.get( fieldStream[i] );
      tableSchema.addField( fieldStream[i], type, len );
    }

    DefineSchema dataConnector =
        new DefineSchema( "ACCESS_MODULE_READER", "DATACONNECTOR PRODUCER", this.meta.getSchemaName() );
    dataConnector.addField( "PrivateLogName", this.meta.getAccessLogFile() );
    dataConnector.addField( "AccessModuleName", "np_axsmod.so" );
    dataConnector.addField( "AccessModuleInitStr", null );
    dataConnector.addField( "FileName", parent != null ? parent.data.fifoFilename : this.meta.getFifoFileName() );
    dataConnector.addField( "Format", "Unformatted" );
    dataConnector.addField( "OpenMode", "Read" );

    // DDL Operator
    DefineSchema ddlOptions = new DefineSchema( "DDL_OPERATOR", "DDL", null );
    ddlOptions.addField( "TdpId             ", this.meta.getDatabaseMeta().getHostname() );
    ddlOptions.addField( "UserName          ", this.meta.getDatabaseMeta().getUsername() );
    ddlOptions.addField( "UserPassword      ", isPreview ? hiddenPassword : this.meta.getDatabaseMeta().getPassword() );
    ddlOptions.addField( "ErrorList         ", "3807" );

    // Update Operator
    DefineSchema updateOptions = new DefineSchema( "UPDATE_OPERATOR", "UPDATE", "*" );
    updateOptions.addField( "TdpId             ", this.meta.getDatabaseMeta().getHostname() );
    updateOptions.addField( "PrivateLogName    ", this.meta.getUpdateLogFile() );
    updateOptions.addField( "UserName          ", this.meta.getDatabaseMeta().getUsername() );
    updateOptions.addField( "UserPassword      ", isPreview ? hiddenPassword : this.meta.getDatabaseMeta()
        .getPassword() );
    updateOptions.addField( "LogTable          ", this.meta.getLogTable() );
    updateOptions.addField( "TargetTable       ", this.meta.getDbName() + "." + this.meta.getTableName() );
    updateOptions.addField( "ErrorTable1       ", this.meta.getErrorTable() );
    updateOptions.addField( "ErrorTable2       ", this.meta.getErrorTable2() );

    // Drop tables as needed......
    String dropTables = null;
    boolean drop = false;
    ApplyClause dropClauses = new ApplyClause();
    if ( this.meta.getDropLogTable() ) {
      dropClauses.addClause( quote( dropTable( this.meta.getLogTable(), "" ) ) );
      drop = true;
    }
    if ( this.meta.getDropWorkTable() ) {
      dropClauses.addClause( quote( dropTable( this.meta.getWorkTable(), "WT" ) ) );
      drop = true;
    }
    if ( this.meta.getDropErrorTable() ) {
      dropClauses.addClause( quote( dropTable( this.meta.getErrorTable(), "ET" ) ) );
      drop = true;
    }
    if ( this.meta.getDropErrorTable2() ) {
      dropClauses.addClause( quote( dropTable( this.meta.getErrorTable2(), "UV" ) ) );
      drop = true;
    }
    if ( drop ) {
      dropTables = createStep( "Setup_tables", dropClauses.toString(), toSelectOperator( "DDL_OPERATOR", null ) );
    }

    // Specific Command
    String loadCommand = null;
    ApplyClause cmdApply = new ApplyClause();
    switch ( this.meta.getActionType() ) {
      case 0:
        cmdApply.addClause( quote( createInsertCommand() ) );
        loadCommand =
            createStep( "Load_Table", cmdApply.toString(), toSelectOperator( "UPDATE_OPERATOR[2]",
                "ACCESS_MODULE_READER[2]" ) );
        break;
      case 1:
        cmdApply.addClause( createUpsertCommand() );
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

  /**
   * @param fileName
   *          the filename to resolve. may contain Kettle Environment variables.
   * @return the data file name.
   * @throws IOException
   *           ...
   */
  @SuppressWarnings( "unused" )
  private String resolveFileName( final String fileName ) throws KettleException {
    final FileObject fileObject = KettleVFS.getFileObject( parent.environmentSubstitute( fileName ) );
    return KettleVFS.getFilename( fileObject );
  }

  /*****************************************
   * formatting routines for "unformatted" output
   ******************************************/

  static byte[] convertChar( ValueMetaInterface valueMeta, Object valueData ) throws KettleValueException {
    String string = valueMeta.getString( valueData );
    if ( string != null ) {
      return ( string.getBytes() );
    }
    return null;
  }

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

  static byte[] convertLong( Long integer ) {
    ByteBuffer b = ByteBuffer.allocate( 8 ).order( ByteOrder.LITTLE_ENDIAN );
    b.putLong( integer != null ? integer : 0 );
    return b.array();
  }

  static byte[] convertFloat( Double d ) {
    ByteBuffer b = ByteBuffer.allocate( 8 ).order( ByteOrder.LITTLE_ENDIAN );
    b.putDouble( d != null ? d : 0 );
    return b.array();
  }

  static byte[] convertBignum( BigDecimal d ) {
    if ( d != null ) {
      return convertFloat( d.doubleValue() );
    } else {
      return convertFloat( new Double( 0 ) );
    }
  }

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
