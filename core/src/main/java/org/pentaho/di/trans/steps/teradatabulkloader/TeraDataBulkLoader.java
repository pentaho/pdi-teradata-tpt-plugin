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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.StreamLogger;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.core.vfs.KettleVFS;

/**
 * Teradata TPT Insert Upsert Bulk Loader<br>
 * <br>
 * Derived from package org.pentaho.di.trans.steps.terafast;<br>
 * Compatible with Kettle 4.4.x <br>
 * Created on 29-oct-2013<br>
 * 
 * @author Kevin Hanrahan<br>
 */

public class TeraDataBulkLoader extends BaseStep implements StepInterface {

  /** The pkg. */
  private static Class<?> PKG = TeraDataBulkLoaderMeta.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$

  /** The Action types. */
  public static String[] ActionTypes = { BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.Insert.Label" ),
    BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.Upsert.Label" ), };

  /** The Script types. */
  public static String[] ScriptTypes = {
    BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.ScriptOptionGenerate.Label" ),
    BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.ScriptOptionUseExisting.Label" ) };

  /** The Constant DEFAULT_ERROR_CODE. */
  public static final long DEFAULT_ERROR_CODE = 1L;

  /** The meta. */
  private TeraDataBulkLoaderMeta meta;

  /** The data. */
  TeraDataBulkLoaderData data;

  /** The thread wait time. */
  private final long threadWaitTime = 300000;

  /** The thread wait time text. */
  private final String threadWaitTimeText = "5min";

  /** The temp script file. */
  private String tempScriptFile;

  /**
   * Instantiates a new tera data bulk loader.
   *
   * @param stepMeta the step meta
   * @param stepDataInterface the step data interface
   * @param copyNr the copy nr
   * @param transMeta the trans meta
   * @param trans the trans
   */
  public TeraDataBulkLoader( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  /**
   * Execute load command.
   *
   * @throws Exception the exception
   */
  private void executeLoadCommand() throws Exception {

    TeraDataBulkLoaderRoutines routines = new TeraDataBulkLoaderRoutines( this, this.meta );
    this.tempScriptFile = routines.createScriptFile();

    data.tbuildThread = new TbuildThread( this );
    data.tbuildThread.start();
    // Ready to start writing rows to the FIFO file now...
    //
    if ( !Const.isWindows() ) {
      logDetailed( BaseMessages.getString( PKG, "TeraDataBulkLoader.Log.OpeningPipe", data.fifoFilename ) );
      OpenFifo openFifo = new OpenFifo( data.fifoFilename, 1000 );
      openFifo.start();

      // Wait for either the sql statement to throw an error or the
      // fifo writer to throw an error
      while ( true ) {
        openFifo.join( 200 );
        if ( openFifo.getState() == Thread.State.TERMINATED ) {
          break;
        }

        try {
          data.tbuildThread.checkExcn();
        } catch ( Exception e ) {
          // We need to open a stream to the fifo to unblock the fifo writer
          // that was waiting for the thread that now isn't running
          new DataInputStream( new FileInputStream( data.fifoFilename ) ).close();
          openFifo.join();
          logError( "Execution error in tbuild: " + e );
          throw e;
        }

        try {
          openFifo.checkExcn();
        } catch ( Exception e ) {
          throw e;
        }
      }
      data.fifoStream = openFifo.getFifoStream();
    }

  }

  /**
   * Execute.
   *
   * @param meta the meta
   * @return true, if successful
   * @throws KettleException the kettle exception
   */
  public boolean execute( TeraDataBulkLoaderMeta meta ) throws KettleException {
    Runtime rt = Runtime.getRuntime();

    try {
      // 1) Create the FIFO file using the "mkfifo" command...
      // Make sure to log all the possible output, also from STDERR
      data.fifoFilename = environmentSubstitute( meta.getFifoFileName() );
      if ( meta.isRandomizeFifoFilename() ) {
        data.fifoFilename += "." + new Random().nextInt( 2139999999 );
      }
      setVariable( "TPT_FIFO_FILENAME", data.fifoFilename );

      File fifoFile = new File( data.fifoFilename );
      if ( !fifoFile.exists() ) {
        // MKFIFO!
        String mkFifoCmd = "mkfifo " + data.fifoFilename;
        logDetailed( BaseMessages.getString( PKG, "TeraDataBulkLoader.Log.CreatePipe", mkFifoCmd ) );
        Process mkFifoProcess = rt.exec( mkFifoCmd );
        StreamLogger errorLogger = new StreamLogger( log, mkFifoProcess.getErrorStream(), "mkFifoError" );
        StreamLogger outputLogger = new StreamLogger( log, mkFifoProcess.getInputStream(), "mkFifoOuptut" );
        new Thread( errorLogger ).start();
        new Thread( outputLogger ).start();
        int result = mkFifoProcess.waitFor();
        if ( result != 0 ) {
          throw new Exception( BaseMessages.getString( PKG,
              "TeraDataBulkLoader.Exception.CommandReturnCodeError", result, mkFifoCmd ) );
        }

        String chmodCmd = "chmod 666 " + data.fifoFilename;
        logDetailed( BaseMessages.getString( PKG, "TeraDataBulkLoader.Log.SetPipePermissions", chmodCmd ) );
        Process chmodProcess = rt.exec( chmodCmd );
        errorLogger = new StreamLogger( log, chmodProcess.getErrorStream(), "chmodError" );
        outputLogger = new StreamLogger( log, chmodProcess.getInputStream(), "chmodOuptut" );
        new Thread( errorLogger ).start();
        new Thread( outputLogger ).start();
        result = chmodProcess.waitFor();
        if ( result != 0 ) {
          throw new Exception( BaseMessages.getString( PKG,
              "TeraDataBulkLoader.Exception.CommandReturnCodeError", result, chmodCmd ) );
        }
      }
      // 3) Now we are ready to run the load command...
      //
      executeLoadCommand();
    } catch ( Exception ex ) {
      throw new KettleException( ex );
    }

    return true;
  }

  /**
   * Create the command line for a tbuild process depending on the meta information supplied.
   * 
   * @return The string to execute.
   * 
   * @throws KettleException
   *           Upon any exception
   */
  public String createCommandLine() throws KettleException {
    if ( StringUtils.isBlank( this.meta.getTbuildPath() ) ) {
      throw new KettleException( BaseMessages.getString( PKG, "TeraDataBulkLoader.Exception.BuildPathNotSet" ) );
    }
    final StringBuilder builder = new StringBuilder();
    try {
      final FileObject fileObject = KettleVFS.getFileObject( environmentSubstitute( this.meta.getTbuildPath() ) );
      final String tbuildExec = KettleVFS.getFilename( fileObject );
      builder.append( tbuildExec + " " );
      // Add command line args as appropriate for generated or existing script
      builder.append( "-f " + this.tempScriptFile + " " );
      if ( !this.meta.getGenerateScript() ) {

        String varfile = environmentSubstitute( this.meta.getVariableFile() );
        if ( varfile != null && !varfile.equals( "" ) ) {
          builder.append( "-v " + varfile + " " );
        }
      }
      builder.append( environmentSubstitute( this.meta.getJobName() ) );
    } catch ( Exception e ) {
      throw new KettleException(
          BaseMessages.getString( PKG, "TeraDataBulkLoader.Exception.ErrorBuildAppString" ), e );
    }
    // Add log error log, if set.
    return builder.toString();
  }

  /**
   * Creates the environment variables.
   *
   * @return the string[]
   */
  public String[] createEnvironmentVariables() {
    List<String> varlist = new ArrayList<String>();
    StringBuffer libpath = new StringBuffer();

    varlist.add( "TWB_ROOT=" + environmentSubstitute( this.meta.getTwbRoot() ) );
    varlist.add( "COPLIB=" + environmentSubstitute( this.meta.getCopLibPath() ) );
    varlist.add( "COPERR=" + environmentSubstitute( this.meta.getCopLibPath() ) );
    libpath.append( environmentSubstitute( this.meta.getLibPath() ) + ":" );
    libpath.append( environmentSubstitute( this.meta.getTbuildLibPath() ) + ":" );
    libpath.append( environmentSubstitute( this.meta.getTdicuLibPath() ) + ":" );
    libpath.append( environmentSubstitute( this.meta.getLibPath() ) + "64:" );
    libpath.append( environmentSubstitute( this.meta.getTbuildLibPath() ) + "64:" );
    libpath.append( environmentSubstitute( this.meta.getTdicuLibPath() ) + "64:" );
    varlist.add( "LD_LIBRARY_PATH=" + libpath.toString() );
    return varlist.toArray( new String[varlist.size()] );
  }

  /**
   * The Class TbuildThread.
   */
  static class TbuildThread extends Thread {

    /** The parent. */
    private TeraDataBulkLoader parent;

    /** The command. */
    private String command;

    /** The environment. */
    private String[] environment;

    /** The process. */
    private Process process;

    /** The exit value. */
    private int exitValue;

    /** The ex. */
    private Exception ex;

    /**
     * Instantiates a new tbuild thread.
     *
     * @param parent the parent
     * @throws KettleException the kettle exception
     */
    TbuildThread( TeraDataBulkLoader parent ) throws KettleException {
      this.parent = parent;
      this.command = parent.createCommandLine();
      this.environment = parent.createEnvironmentVariables();

    }

    /* (non-Javadoc)
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
      StringBuilder errors = new StringBuilder();

      parent.logBasic( BaseMessages.getString( PKG, "TeraDataBulkLoader.Log.RunCommand", command ) );
      parent.logBasic( BaseMessages.getString( PKG,
          "TeraDataBulkLoader.Log.Environment", StringUtils.join( environment, ":" ) ) );

      try {
        this.process = Runtime.getRuntime().exec( command, environment );
        InputStream tbuildOutput = this.process.getInputStream();
        DataInputStream in = new DataInputStream( tbuildOutput );
        BufferedReader br = new BufferedReader( new InputStreamReader( in ) );
        String strLine;
        while ( ( strLine = br.readLine() ) != null ) {
          parent.logDetailed( strLine );
          if ( strLine.matches( "(?i:.*ERROR.*)" ) ) {
            errors.append( strLine + "\n" );
          }
        }
        exitValue = process.waitFor();
        if ( exitValue > 0 ) {
          this.ex = new KettleException( BaseMessages.getString( PKG,
              "TeraDataBulkLoader.Exception.TBuildProcessError", exitValue, errors.toString() ) );
        }
      } catch ( Exception e ) {
        this.ex = e;
      }

    }

    /**
     * Check excn.
     *
     * @throws Exception the exception
     */
    void checkExcn() throws Exception {
      // This is called from the main thread context to rethrow any saved
      // excn.
      if ( ex != null ) {
        throw ex;
      }
    }
  }

  /** 
   * Process individual incoming rows
   */
  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    meta = (TeraDataBulkLoaderMeta) smi;
    data = (TeraDataBulkLoaderData) sdi;

    try {

      Object[] r = getRow(); // Get row from input rowset & set row busy!
      if ( r == null ) {
        // no more input to be expected...
        closeOutput();
        setOutputDone();
        return false;
      }

      if ( first ) {
        first = false;
        // Cache field indexes.
        //
        data.keynrs = new int[meta.getFieldStream().length];
        for ( int i = 0; i < data.keynrs.length; i++ ) {
          data.keynrs[i] = getInputRowMeta().indexOfValue( meta.getFieldStream()[i] );
        }

        data.bulkFormatMeta = new ValueMetaInterface[data.keynrs.length];
        // execute the client statement...
        //
        execute( meta );
      }

      writeRowToBulk( getInputRowMeta(), r );
      putRow( getInputRowMeta(), r );
      incrementLinesOutput();
      return true;
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "TeraDataBulkLoader.Log.ErrorInStep" ), e );
      setErrors( 1 );
      stopAll();
      setOutputDone(); // signal end to receiver(s)
      return false;
    }
  }

  /**
   * Close output.
   *
   * @throws Exception the exception
   */
  private void closeOutput() throws Exception {

    if ( data.fifoStream != null ) {
      // Close the fifo file...
      //
      data.fifoStream.close();
      data.fifoStream = null;
    }
    if ( data.tbuildThread != null ) {

      // wait for the thread to finish and check for any error and/or warning...
      logBasic( BaseMessages.getString( PKG, "TeraDataBulkLoader.Log.WaitForTBuild", this.threadWaitTimeText ) );
      data.tbuildThread.join( this.threadWaitTime );
      TbuildThread tbuildThread = data.tbuildThread;
      data.tbuildThread = null;
      tbuildThread.checkExcn();
    }
  }

  /**
   * Write row to bulk.
   *
   * @param rowMeta the row meta
   * @param r the r
   * @throws KettleException the kettle exception
   */
  private void writeRowToBulk( RowMetaInterface rowMeta, Object[] r ) throws KettleException {

    try {
      for ( int i = 0; i < data.keynrs.length; i++ ) {
        int index = data.keynrs[i];
        ValueMetaInterface valueMeta = rowMeta.getValueMeta( index );
        Object valueData = r[index];

        switch ( valueMeta.getType() ) {
          case ValueMetaInterface.TYPE_STRING:
            data.fifoStream.write( TeraDataBulkLoaderRoutines.convertVarchar( valueMeta.getString( valueData ) ) );
            break;
          case ValueMetaInterface.TYPE_INTEGER:
            data.fifoStream.write( TeraDataBulkLoaderRoutines.convertLong( valueMeta.getInteger( valueData ) ) );
            break;
          case ValueMetaInterface.TYPE_DATE:
            Date date = valueMeta.getDate( valueData );
            data.fifoStream.write( TeraDataBulkLoaderRoutines.convertDateTime( date ) );
            break;
          case ValueMetaInterface.TYPE_BOOLEAN:
            Boolean b = valueMeta.getBoolean( valueData );
            data.fifoStream.write( TeraDataBulkLoaderRoutines.convertBoolean( b ) );
            break;
          case ValueMetaInterface.TYPE_NUMBER:
            Double d = valueMeta.getNumber( valueData );
            data.fifoStream.write( TeraDataBulkLoaderRoutines.convertFloat( d ) );
            break;
          case ValueMetaInterface.TYPE_BIGNUMBER:
            BigDecimal bn = valueMeta.getBigNumber( valueData );
            data.fifoStream.write( TeraDataBulkLoaderRoutines.convertBignum( bn ) );
            break;
          default:
            logError( BaseMessages.getString( PKG, "TeraDataBulkLoader.Log.UnsupportedType", valueMeta.getType() ) );
            throw new KettleException( BaseMessages.getString( PKG,
                "TeraDataBulkLoader.Exception.UnsupportedType", valueMeta.getType() ) );
        }
      }

    } catch ( IOException e ) {
      // If something went wrong with writing to the fifo, get the underlying error from MySQL
      try {
        logError( BaseMessages.getString( PKG, "TeraDataBulkLoader.Log.ErrorDuringWrite", this.threadWaitTimeText ) );
      } catch ( Exception loadEx ) {
        logError( BaseMessages.getString( PKG, "TeraDataBulkLoader.Log.LoadexError", loadEx ) );
        throw new KettleException( "loadEx Error serializing rows of data to the fifo file 1", loadEx );
      }

      // throw the generic "Pipe" exception.
      logError( BaseMessages.getString( PKG, "TeraDataBulkLoader.Log.IOError" ), e );
      throw new KettleException( BaseMessages.getString( PKG, "TeraDataBulkLoader.Exception.IOError" ), e );

    } catch ( Exception e2 ) {
      logError( BaseMessages.getString( PKG, "TeraDataBulkLoader.Log.UnknownError" ), e2 );
      // Null pointer exceptions etc.
      throw new KettleException( BaseMessages.getString( PKG, "TeraDataBulkLoader.Exception.UnknownError" ), e2 );
    }
  }

  protected void verifyDatabaseConnection() throws KettleException {
    // Confirming Database Connection is defined.
    if ( meta.getDatabaseMeta() == null ) {
      throw new KettleException( BaseMessages.getString( PKG, "TeraDataBulkLoaderMeta.Exception.NoConnectionDefined" ) );
    }
  }

  /**
   * Initialize this step
   */
  @Override
  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (TeraDataBulkLoaderMeta) smi;
    data = (TeraDataBulkLoaderData) sdi;

    if ( super.init( smi, sdi ) ) {
      try {
        verifyDatabaseConnection();
      } catch ( KettleException ex ) {
        logError( ex.getMessage() );
        return false;
      }
      return true;
    }
    return false;
  }

  /**
   * Dispose of this step (called as a cleanup method)
   */
  @Override
  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (TeraDataBulkLoaderMeta) smi;
    data = (TeraDataBulkLoaderData) sdi;

    // Close the output streams if still needed.
    //
    try {
      if ( data.fifoStream != null ) {
        data.fifoStream.close();
      }

      if ( data.db != null ) {
        data.db.disconnect();
        data.db = null;
      }

      // remove the fifo file...
      //
      try {
        if ( data.fifoFilename != null ) {
          new File( data.fifoFilename ).delete();
        }
      } catch ( Exception e ) {
        logError( BaseMessages.getString( PKG, "TeraDataBulkLoader.Log.CannotDeletePipe", data.fifoFilename ), e );
      }
    } catch ( Exception e ) {
      setErrors( 1L );
      logError( BaseMessages.getString( PKG, "TeraDataBulkLoader.Log.CloseConnectionError" ), e );
    }

    super.dispose( smi, sdi );
  }

  /**
   *  Class to try and open a writer to a fifo in a different thread.
   *  Opening the fifo is a blocking call, so we need to check for errors
   */
  static class OpenFifo extends Thread {

    /** The fifo stream. */
    private DataOutputStream fifoStream = null;

    /** The ex. */
    private Exception ex;

    /** The fifo name. */
    private String fifoName;

    /** The size. */
    @SuppressWarnings( "unused" )
    private int size;

    /**
     * Instantiates a new open fifo.
     *
     * @param fifoName the fifo name
     * @param size the size
     */
    OpenFifo( String fifoName, int size ) {
      this.fifoName = fifoName;
      this.size = size;
    }

    /* (non-Javadoc)
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
      try {
        fifoStream = new DataOutputStream( new FileOutputStream( OpenFifo.this.fifoName ) );
      } catch ( Exception ex ) {
        this.ex = ex;
      }
    }

    /**
     * Check exception.
     *
     * @throws Exception the exception
     */
    void checkExcn() throws Exception {
      // This is called from the main thread context to rethrow any saved
      // excn.
      if ( ex != null ) {
        throw ex;
      }
    }

    /**
     * Gets the fifo stream.
     *
     * @return the fifo stream
     */
    DataOutputStream getFifoStream() {
      return fifoStream;
    }
  }

}
