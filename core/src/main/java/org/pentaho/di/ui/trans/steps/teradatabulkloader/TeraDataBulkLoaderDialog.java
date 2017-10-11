/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.ui.trans.steps.teradatabulkloader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.RowLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.SourceToTargetMapping;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.teradatabulkloader.TeraDataBulkLoader;
import org.pentaho.di.trans.steps.teradatabulkloader.TeraDataBulkLoaderMeta;
import org.pentaho.di.trans.steps.teradatabulkloader.TeraDataBulkLoaderRoutines;
import org.pentaho.di.ui.core.dialog.EnterMappingDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.trans.step.TableItemInsertListener;

/**
 * Dialog class for the TeraData bulk loader step.
 * 
 */
public class TeraDataBulkLoaderDialog extends BaseStepDialog implements StepDialogInterface {

  /** The pkg. */
  static Class<?> PKG = TeraDataBulkLoaderMeta.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$

  /** The Constant NO_BUTTON. */
  static final int NO_BUTTON = 0;

  /** The Constant FILE_BUTTON. */
  static final int FILE_BUTTON = 1;

  /** The Constant DIR_BUTTON. */
  static final int DIR_BUTTON = 2;

  /** The Constant TABLE_BUTTON. */
  static final int TABLE_BUTTON = 4;

  /** The Constant LABEL_ONLY. */
  static final int LABEL_ONLY = 8;

  /** The Constant NO_VAR. */
  static final int NO_VAR = 16;

  private Integer lastTypeSelection = -1;

  private final List<DialogPopulator> dialogPopulators = new ArrayList<DialogPopulator>();

  /* The "useful" widgets are listed first for clarity. Associated labels, buttons form data follow...... */
  /** ************************* These variables are common to both generated and existing scripts **************. */
  CCombo wConnection;

  /** The item set. */
  CTabFolder fItemSet = null;

  /* the remainder go into "Script Configuration" tab */
  /** The c script option. */
  private RadioComposite cScriptOption; // (generateScript) - single boolean variable

  /** ************************* These variables are for using a pre-existing control file **************. */

  /** ************************* These variables are for using a generated control file **************. */

  /** The w preview script. */
  private Button wPreviewScript;

  /** The wb control file. */
  CompositeMenuItem wbControlFile;

  /** The input. */
  TeraDataBulkLoaderMeta input;

  /** ********************************** associated widgets for generated script ****************. */

  /** List of ColumnInfo that should have the field names of the selected database table. */
  private List<ColumnInfo> tableFieldColumns = new ArrayList<ColumnInfo>();

  private CompositeMenuItem wScriptOption;

  int margin = Const.MARGIN;

  /**
   * Instantiates a new tera data bulk loader dialog.
   * 
   * @param parent
   *          the parent
   * @param in
   *          the in
   * @param transMeta
   *          the trans meta
   * @param sname
   *          the sname
   */
  public TeraDataBulkLoaderDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (TeraDataBulkLoaderMeta) in;
  }

  /** The ls mod. */
  ModifyListener lsMod = new ModifyListener() {
    @Override
    public void modifyText( ModifyEvent e ) {
      input.setChanged();
    }
  };

  /** The ls mod select. */
  Listener lsModSelect = new Listener() {
    @Override
    public void handleEvent( Event e ) {
      input.setChanged();
    }
  };

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.di.trans.step.StepDialogInterface#open()
   */
  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.Stepname.Label" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( middle, -margin );
    fdlStepname.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( 0, margin );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );

    // Connection line
    Label wlConnection = new Label( shell, SWT.RIGHT );
    wConnection =
        addConnectionLine( shell, wStepname, middle, margin, wlConnection, new Button( shell, SWT.PUSH ), new Button(
            shell, SWT.PUSH ), new Button( shell, SWT.PUSH ), null );
    if ( input.getDatabaseMeta() == null && transMeta.nrDatabases() == 1 ) {
      wConnection.select( 0 );
    }
    wConnection.addModifyListener( lsMod );
    wlConnection.setText( BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.Connection.Label" ) );

    // Generate or Use Script File
    wScriptOption =
        new CompositeMenuItem( props, lsMod, lsModSelect, input, shell, wConnection,
            "TeraDataBulkLoaderDialog.ScriptOption.Label", 0 );
    cScriptOption =
        wScriptOption.addRadioComposite( TeraDataBulkLoader.ScriptTypes, SWT.NO_RADIO_GROUP, this, new Runnable() {

          @Override
          public void run() {
            createDynamicTabs();
          }
        } );
    // Composite groups and folder
    // Shell contains cCommonItems and fItemSet folder.
    // fItemSet folder contains Script and Execution tabs, and the composites cExecutionScriptItems and cExecutionItems

    fItemSet = new CTabFolder( shell, SWT.BORDER );
    props.setLook( fItemSet, Props.WIDGET_STYLE_TAB );

    /******************************************************************************************************************/
    /*************** cCommonItems - Common items Composite Group ******************************************************/
    /******************************************************************************************************************/

    // THE BUTTONS
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    // Preview Button
    wPreviewScript = new Button( shell, SWT.PUSH );
    wPreviewScript.setText( BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.Preview.Button" ) );

    setButtonPositions( new Button[] { wOK, wPreviewScript, wCancel }, margin, null );

    FormData fdItemSet = new FormData();
    fdItemSet.left = new FormAttachment( 0, 0 );
    fdItemSet.top = new FormAttachment( wScriptOption.getComposite(), margin );
    fdItemSet.right = new FormAttachment( 100, 0 );
    fdItemSet.bottom = new FormAttachment( wOK, -margin );
    fItemSet.setLayoutData( fdItemSet );
    props.setLook( fItemSet );

    lsOK = new Listener() {
      @Override
      public void handleEvent( Event e ) {
        ok();
      }
    };

    lsCancel = new Listener() {
      @Override
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    lsPreview = new Listener() {
      @Override
      public void handleEvent( Event e ) {
        previewScript();
      }
    };

    wOK.addListener( SWT.Selection, lsOK );
    wCancel.addListener( SWT.Selection, lsCancel );
    wPreviewScript.addListener( SWT.Selection, lsPreview );
    lsDef = new SelectionAdapter() {
      @Override
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      @Override
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    getData();
    fItemSet.pack();
    setSize();
    input.setChanged( changed );
    shell.open();

    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  private InputFields getInputFields() {
    Map<String, Integer> inputFieldLength = new HashMap<String, Integer>();
    Map<String, Integer> inputFields = new HashMap<String, Integer>();
    Map<String, Integer> inputFieldType = new HashMap<String, Integer>();

    StepMeta stepMeta = transMeta.findStep( stepname );
    if ( stepMeta != null ) {
      try {
        RowMetaInterface row = transMeta.getPrevStepFields( stepMeta );

        // Remember these fields...
        for ( int i = 0; i < row.size(); i++ ) {
          ValueMetaInterface valueMetaInterface = row.getValueMeta( i );
          inputFields.put( valueMetaInterface.getName(), Integer.valueOf( i ) );
          inputFieldType.put( valueMetaInterface.getName(), valueMetaInterface.getType() );
          inputFieldLength.put( valueMetaInterface.getName(), valueMetaInterface.getLength() );
        }
      } catch ( KettleException e ) {
        logError( BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Message" ) );
      }
    }
    return new InputFields( inputFields, inputFieldType, inputFieldLength );
  }

  private GeneralTabWidgets createExecutionTab() {
    CTabItem tiExecutionItems = new CTabItem( fItemSet, SWT.NONE );
    Composite cExecutionItems = new Composite( fItemSet, SWT.BORDER );
    props.setLook( cExecutionItems );
    tiExecutionItems.setControl( cExecutionItems );
    tiExecutionItems.setText( BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.ExecutionTab.Label" ) );
    /******************************************************************************************************************/
    /*************** cExecutionItems -- Execution Environment Composite Group *****************************************/
    /******************************************************************************************************************/
    Group locationsGroup = new Group( cExecutionItems, SWT.SHADOW_NONE );
    props.setLook( locationsGroup );
    locationsGroup.setText( BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.LocationGroup.Label" ) );
    FormLayout locationsGroupLayout = new FormLayout();
    locationsGroupLayout.marginWidth = 10;
    locationsGroupLayout.marginHeight = 10;
    locationsGroup.setLayout( locationsGroupLayout );
    FormData locationsData = new FormData();
    locationsData.left = new FormAttachment( 0, margin );
    locationsData.top = new FormAttachment( 0, 2 * margin );
    locationsData.right = new FormAttachment( 100, -margin );
    locationsGroup.setLayoutData( locationsData );

    final TextVarMenuItem wTdInstall =
        new DirectoryTextVarMenuItem( shell, locationsGroup, props, transMeta, lsMod, (Control) null,
            "TeraDataBulkLoaderDialog.TdInstall.Label" );
    final TextVarMenuItem wTwbRoot =
        new DirectoryTextVarMenuItem( shell, locationsGroup, props, transMeta, lsMod, wTdInstall,
            "TeraDataBulkLoaderDialog.TwbRoot.Label" );
    final TextVarMenuItem wLibPath =
        new DirectoryTextVarMenuItem( shell, locationsGroup, props, transMeta, lsMod, wTwbRoot,
            "TeraDataBulkLoaderDialog.TDLibPath.Label" );
    final TextVarMenuItem wTbuildLibPath =
        new DirectoryTextVarMenuItem( shell, locationsGroup, props, transMeta, lsMod, wLibPath,
            "TeraDataBulkLoaderDialog.TbuildLibPath.Label" );
    final TextVarMenuItem wTdicuLibPath =
        new DirectoryTextVarMenuItem( shell, locationsGroup, props, transMeta, lsMod, wTbuildLibPath,
            "TeraDataBulkLoaderDialog.TdicuLibPath.Label" );
    final TextVarMenuItem wCopLibPath =
        new DirectoryTextVarMenuItem( shell, locationsGroup, props, transMeta, lsMod, wTdicuLibPath,
            "TeraDataBulkLoaderDialog.CopLibPath.Label" );
    final TextVarMenuItem wTbuildPath =
        new FileTextVarMenuItem( shell, locationsGroup, props, transMeta, lsMod, wCopLibPath,
            "TeraDataBulkLoaderDialog.TbuildPath.Label" );

    Group otherGroup = new Group( cExecutionItems, SWT.SHADOW_NONE );
    props.setLook( otherGroup );
    otherGroup.setText( BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.OtherGroup.Label" ) );
    FormLayout otherGroupLayout = new FormLayout();
    otherGroupLayout.marginWidth = 10;
    otherGroupLayout.marginHeight = 10;
    otherGroup.setLayout( otherGroupLayout );

    FormData otherData = new FormData();
    otherData.top = new FormAttachment( locationsGroup, margin );
    otherData.left = new FormAttachment( 0, margin );
    otherData.right = new FormAttachment( 100, -margin );
    otherData.bottom = new FormAttachment( 100, -margin );
    otherGroup.setLayoutData( otherData );

    final TextVarMenuItem wDataFile =
        new NoButtonTextVarMenuItem( otherGroup, props, transMeta, lsMod, wTbuildPath,
            "TeraDataBulkLoaderDialog.DataFile.Label" );

    CompositeMenuItem wRandomizeFifoName =
        new CompositeMenuItem( props, lsMod, lsModSelect, input, otherGroup, wDataFile.getTextVar(),
            "TeraDataBulkLoaderDialog.DataFileRandomize.Label", 0 );
    final Button randomizeFifoName = wRandomizeFifoName.addButton( "", SWT.CHECK );

    final TextVarMenuItem wJobName =
        new NoButtonTextVarMenuItem( otherGroup, props, transMeta, lsMod, wRandomizeFifoName,
            "TeraDataBulkLoaderDialog.JobName.Label" );

    cExecutionItems.setLayout( new FormLayout() );
    FormData fdExecutionItems = new FormData();
    fdExecutionItems.left = new FormAttachment( 0, margin );
    fdExecutionItems.top = new FormAttachment( wScriptOption.getComposite(), margin );
    fdExecutionItems.right = new FormAttachment( 100, -margin );
    cExecutionItems.setLayoutData( fdExecutionItems );
    dialogPopulators.add( new DialogPopulator() {

      @Override
      public void validate( List<String> errors ) {
        addIfTrue( errors, Const.isEmpty( wTdInstall.getText() ),
            "TeraDataBulkLoaderDialog.MissingInstallPath.DialogMessage" );
      }

      @Override
      public void populateMeta( TeraDataBulkLoaderMeta inf ) {
        inf.setTdInstallPath( wTdInstall.getText() );
        inf.setTwbRoot( wTwbRoot.getText() );
        inf.setLibPath( wLibPath.getText() );
        inf.setTbuildLibPath( wTbuildLibPath.getText() );
        inf.setTdicuLibPath( wTdicuLibPath.getText() );
        inf.setCopLibPath( wCopLibPath.getText() );
        inf.setTbuildPath( wTbuildPath.getText() );
        inf.setFifoFileName( wDataFile.getText() );
        inf.setJobName( wJobName.getText() );
        inf.setRandomizeFifoFilename( randomizeFifoName.getSelection() );
      }

      @Override
      public void populateDialog( TeraDataBulkLoaderMeta input ) {
        String val;
        if ( ( val = input.getTdInstallPath() ) != null ) {
          wTdInstall.setText( val );
        }
        if ( ( val = input.getTwbRoot() ) != null ) {
          wTwbRoot.setText( val );
        }
        if ( ( val = input.getLibPath() ) != null ) {
          wLibPath.setText( val );
        }
        if ( ( val = input.getTbuildLibPath() ) != null ) {
          wTbuildLibPath.setText( val );
        }
        if ( ( val = input.getTdicuLibPath() ) != null ) {
          wTdicuLibPath.setText( val );
        }
        if ( ( val = input.getCopLibPath() ) != null ) {
          wCopLibPath.setText( val );
        }
        if ( ( val = input.getTbuildPath() ) != null ) {
          wTbuildPath.setText( val );
        }
        if ( ( val = input.getFifoFileName() ) != null ) {
          wDataFile.setText( val );
        }
        if ( ( val = input.getJobName() ) != null ) {
          wJobName.setText( val );
        }
        randomizeFifoName.setSelection( input.isRandomizeFifoFilename() );
      }
    } );
    return new GeneralTabWidgets( tiExecutionItems, randomizeFifoName );
  }

  private Button createCheckbox( Composite parent, int style, String label, Listener listener ) {
    Button button = new Button( parent, style | SWT.CHECK );
    if ( !Const.isEmpty( label ) ) {
      button.setText( BaseMessages.getString( TeraDataBulkLoaderDialog.PKG, label ) );
    }
    button.addListener( SWT.CHECK, listener );
    props.setLook( button );
    return button;
  }

  private void createGenScriptTabs() {
    CTabItem tiGenScriptControlItems = new CTabItem( fItemSet, SWT.NONE );
    tiGenScriptControlItems.setText( BaseMessages.getString( PKG,
        "TeraDataBulkLoaderDialog.GenerateScriptControlTab.Label" ) );
    final Composite cGenScriptControlItems = new Composite( fItemSet, SWT.BORDER );
    props.setLook( cGenScriptControlItems );
    /**************************** these in the control tab *****************************************/
    final TextVarMenuItem wSchema =
        new NoButtonTextVarMenuItem( cGenScriptControlItems, props, transMeta, lsMod, (Control) null,
            "TeraDataBulkLoaderDialog.TargetSchema.Label" );
    final TextVarMenuItem wTable =
        new TableTextVarMenuItem( shell, cGenScriptControlItems, props, transMeta, lsMod, wSchema,
            "TeraDataBulkLoaderDialog.TargetTable.Label" );
    final Runnable setTableFieldCombo = new Runnable() {

      @Override
      public void run() {
        setTableFieldCombo( wSchema, wTable );
      }
    };
    ModifyListener lsTableMod = new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent arg0 ) {
        input.setChanged();
        setTableFieldCombo.run();
      }
    };
    wSchema.getTextVar().addModifyListener( lsTableMod );
    ( (Button) wTable.getButton() ).addSelectionListener( new TableSelectionButtonListener( transMeta, wTable
        .getTextVar(), wConnection, shell, wSchema, wTable, setTableFieldCombo, log ) );
    wTable.getTextVar().addModifyListener( lsTableMod );
    final TextVarMenuItem wLogTable =
        new NoButtonTextVarMenuItem( cGenScriptControlItems, props, transMeta, lsMod, wTable,
            "TeraDataBulkLoaderDialog.LogTablePrompt.Label" );
    final TextVarMenuItem wWorkTable =
        new NoButtonTextVarMenuItem( cGenScriptControlItems, props, transMeta, lsMod, wLogTable,
            "TeraDataBulkLoaderDialog.WorkTablePrompt.Label" );
    final TextVarMenuItem wErrorTable =
        new NoButtonTextVarMenuItem( cGenScriptControlItems, props, transMeta, lsMod, wWorkTable,
            "TeraDataBulkLoaderDialog.ErrorTablePrompt.Label" );
    final TextVarMenuItem wErrorTable2 =
        new NoButtonTextVarMenuItem( cGenScriptControlItems, props, transMeta, lsMod, wErrorTable,
            "TeraDataBulkLoaderDialog.ErrorTable2Prompt.Label" );
    final TextVarMenuItem wScriptFile =
        new NoButtonTextVarMenuItem( cGenScriptControlItems, props, transMeta, lsMod, wErrorTable2,
            "TeraDataBulkLoaderDialog.ScriptFile.Label" );

    Group dropTablesBeforeLoadGroup = new Group( cGenScriptControlItems, SWT.SHADOW_NONE );
    props.setLook( dropTablesBeforeLoadGroup );
    dropTablesBeforeLoadGroup.setText( BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.DropTables.Label" ) );
    dropTablesBeforeLoadGroup.setLayout( new FormLayout() );
    FormData dropTablesBeforeLoadGroupData = new FormData();
    dropTablesBeforeLoadGroupData.left = new FormAttachment( 0, margin );
    dropTablesBeforeLoadGroupData.top = new FormAttachment( wScriptFile.getTextVar(), 2 * margin );
    dropTablesBeforeLoadGroupData.right = new FormAttachment( 100, -margin );
    dropTablesBeforeLoadGroup.setLayoutData( dropTablesBeforeLoadGroupData );
    Composite dropTablesBeforeLoadComposite = new Composite( dropTablesBeforeLoadGroup, SWT.NONE );
    props.setLook( dropTablesBeforeLoadComposite );
    RowLayout dropTablesBeforeLoadLayout = new RowLayout();
    dropTablesBeforeLoadLayout.spacing = 10;
    dropTablesBeforeLoadComposite.setLayout( dropTablesBeforeLoadLayout );
    FormData dropTablesBeforeLoadData = new FormData();
    dropTablesBeforeLoadData.left = new FormAttachment( props.getMiddlePct(), 0 );
    dropTablesBeforeLoadData.top = new FormAttachment( 0, 0 );
    dropTablesBeforeLoadData.right = new FormAttachment( 100, 0 );
    dropTablesBeforeLoadComposite.setLayoutData( dropTablesBeforeLoadData );
    final Button wbDropLog =
        createCheckbox( dropTablesBeforeLoadComposite, SWT.RIGHT, "TeraDataBulkLoaderDialog.LogTable.Label",
            lsModSelect );
    final Button wbDropWork =
        createCheckbox( dropTablesBeforeLoadComposite, SWT.RIGHT, "TeraDataBulkLoaderDialog.WorkTable.Label",
            lsModSelect );
    final Button wbDropError =
        createCheckbox( dropTablesBeforeLoadComposite, SWT.RIGHT, "TeraDataBulkLoaderDialog.ErrorTable.Label",
            lsModSelect );
    final Button wbDropError2 =
        createCheckbox( dropTablesBeforeLoadComposite, SWT.RIGHT, "TeraDataBulkLoaderDialog.ErrorTable2.Label",
            lsModSelect );

    Group rowHandlingGroupWrapper = new Group( cGenScriptControlItems, SWT.SHADOW_NONE );
    rowHandlingGroupWrapper.setText( BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.RowHandling.Label" ) );
    props.setLook( rowHandlingGroupWrapper );
    rowHandlingGroupWrapper.setLayout( new FormLayout() );
    FormData rowHandlingGroupWrapperData = new FormData();
    rowHandlingGroupWrapperData.left = new FormAttachment( 0, margin );
    rowHandlingGroupWrapperData.top = new FormAttachment( dropTablesBeforeLoadGroup, margin );
    rowHandlingGroupWrapperData.right = new FormAttachment( 100, -margin );
    rowHandlingGroupWrapper.setLayoutData( rowHandlingGroupWrapperData );
    Composite rowHandlingComposite = new Composite( rowHandlingGroupWrapper, SWT.NONE );
    props.setLook( rowHandlingComposite );
    RowLayout rowHandlingCompositeLayout = new RowLayout();
    rowHandlingCompositeLayout.spacing = 10;
    rowHandlingComposite.setLayout( rowHandlingCompositeLayout );
    FormData rowHandlingCompositeLayoutData = new FormData();
    rowHandlingCompositeLayoutData.left = new FormAttachment( props.getMiddlePct(), 0 );
    rowHandlingCompositeLayoutData.top = new FormAttachment( 0, 0 );
    rowHandlingCompositeLayoutData.right = new FormAttachment( 100, 0 );
    rowHandlingComposite.setLayoutData( rowHandlingCompositeLayoutData );
    final Button wbIgnoreDupUpdate =
        createCheckbox( rowHandlingComposite, SWT.RIGHT, "TeraDataBulkLoaderDialog.IgnoreDupUpdate.Label", lsModSelect );
    final Button wbInsertMissingUpdate =
        createCheckbox( rowHandlingComposite, SWT.RIGHT, "TeraDataBulkLoaderDialog.InsertMissingUpdate.Label",
            lsModSelect );
    final Button wbIgnoreMissingUpdate =
        createCheckbox( rowHandlingComposite, SWT.RIGHT, "TeraDataBulkLoaderDialog.IgnoreMissing.Label", lsModSelect );

    Group logFilesGroup = new Group( cGenScriptControlItems, SWT.SHADOW_NONE );
    props.setLook( logFilesGroup );
    logFilesGroup.setText( BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.LogFiles.Label" ) );
    FormLayout logFilesGroupLayout = new FormLayout();
    logFilesGroupLayout.marginWidth = 10;
    logFilesGroupLayout.marginHeight = 10;
    logFilesGroup.setLayout( logFilesGroupLayout );
    FormData logFilesGroupData = new FormData();
    logFilesGroupData.left = new FormAttachment( 0, margin );
    logFilesGroupData.top = new FormAttachment( rowHandlingGroupWrapper, 2 * margin );
    logFilesGroupData.right = new FormAttachment( 100, -margin );
    logFilesGroup.setLayoutData( logFilesGroupData );

    final TextVarMenuItem wAccessLogFile =
        new FileTextVarMenuItem( shell, logFilesGroup, props, transMeta, lsMod, rowHandlingComposite,
            "TeraDataBulkLoaderDialog.AccessLogFile.Label" );
    final TextVarMenuItem wUpdateLogFile =
        new FileTextVarMenuItem( shell, logFilesGroup, props, transMeta, lsMod, wAccessLogFile,
            "TeraDataBulkLoaderDialog.UpdateLogFile.Label" );
    tiGenScriptControlItems.setControl( cGenScriptControlItems );
    cGenScriptControlItems.setLayout( new FormLayout() );
    dialogPopulators.add( new DialogPopulator() {

      @Override
      public void validate( List<String> errors ) {
        addIfTrue( errors, Const.isEmpty( wSchema.getText() ), "TeraDataBulkLoaderDialog.MissingSchema.DialogMessage" );
        addIfTrue( errors, Const.isEmpty( wTable.getText() ),
            "TeraDataBulkLoaderDialog.MissingTargetTable.DialogMessage" );
        addIfTrue( errors, Const.isEmpty( wLogTable.getText() ),
            "TeraDataBulkLoaderDialog.MissingLogTable.DialogMessage" );
      }

      @Override
      public void populateMeta( TeraDataBulkLoaderMeta inf ) {
        inf.setSchemaName( wSchema.getText() );
        inf.setTableName( wTable.getText() );
        inf.setLogTable( wLogTable.getText() );
        inf.setWorkTable( wWorkTable.getText() );
        inf.setErrorTable( wErrorTable.getText() );
        inf.setErrorTable2( wErrorTable2.getText() );
        inf.setDropLogTable( wbDropLog.getSelection() );
        inf.setDropWorkTable( wbDropWork.getSelection() );
        inf.setDropErrorTable( wbDropError.getSelection() );
        inf.setDropErrorTable2( wbDropError2.getSelection() );
        inf.setIgnoreDupUpdate( wbIgnoreDupUpdate.getSelection() );
        inf.setInsertMissingUpdate( wbInsertMissingUpdate.getSelection() );
        inf.setIgnoreMissingUpdate( wbIgnoreMissingUpdate.getSelection() );
        inf.setAccessLogFile( wAccessLogFile.getText() );
        inf.setUpdateLogFile( wUpdateLogFile.getText() );
        inf.setScriptFileName( wScriptFile.getText() );
      }

      @Override
      public void populateDialog( TeraDataBulkLoaderMeta input ) {
        String val;
        if ( ( val = input.getSchemaName() ) != null ) {
          wSchema.setText( val );
        }
        if ( ( val = input.getTableName() ) != null ) {
          wTable.setText( val );
        }
        if ( ( val = input.getLogTable() ) != null ) {
          wLogTable.setText( val );
        }
        if ( ( val = input.getWorkTable() ) != null ) {
          wWorkTable.setText( val );
        }
        if ( ( val = input.getErrorTable() ) != null ) {
          wErrorTable.setText( val );
        }
        if ( ( val = input.getErrorTable2() ) != null ) {
          wErrorTable2.setText( val );
        }
        if ( ( val = input.getAccessLogFile() ) != null ) {
          wAccessLogFile.setText( val );
        }
        if ( ( val = input.getUpdateLogFile() ) != null ) {
          wUpdateLogFile.setText( val );
        }
        if ( ( val = input.getScriptFileName() ) != null ) {
          wScriptFile.setText( val );
        }

        wbDropLog.setSelection( input.getDropLogTable() );
        wbDropWork.setSelection( input.getDropWorkTable() );
        wbDropError.setSelection( input.getDropErrorTable() );
        wbDropError2.setSelection( input.getDropErrorTable2() );
        wbIgnoreDupUpdate.setSelection( input.getIgnoreDupUpdate() );
        wbInsertMissingUpdate.setSelection( input.getInsertMissingUpdate() );
        wbIgnoreMissingUpdate.setSelection( input.getIgnoreMissingUpdate() );
      }
    } );

    tableFieldColumns.clear();
    CTabItem tiGenScriptFields = new CTabItem( fItemSet, SWT.NONE );
    tiGenScriptFields.setText( BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.GenerateScriptFieldsTab.Label" ) );
    Composite cGenScriptFields = new Composite( fItemSet, SWT.BORDER );
    props.setLook( cGenScriptFields );

    /**************************** these in the fields tab *****************************************/
    CompositeMenuItem wActionType =
        new CompositeMenuItem( props, lsMod, lsModSelect, input, cGenScriptFields, (Control) null,
            "TeraDataBulkLoaderDialog.ActionType.Label", 0 );

    final RadioComposite cActionType =
        wActionType.addRadioComposite( TeraDataBulkLoader.ActionTypes, SWT.NO_RADIO_GROUP, this, null );
    // Key table
    int nrKeyCols = 3;
    int nrKeyRows =
        ( ( input.getKeyStream() != null ) && ( input.getKeyStream().length > 3 ) ? input.getKeyStream().length : 3 );

    final Label wlKey = new Label( cGenScriptFields, SWT.NONE );
    wlKey.setText( BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.Keys.Label" ) );
    props.setLook( wlKey );
    FormData fdlKey = new FormData();
    fdlKey.left = new FormAttachment( 0, 0 );
    fdlKey.top = new FormAttachment( wActionType.getComposite(), margin );
    wlKey.setLayoutData( fdlKey );

    final ColumnInfo[] ciKey = new ColumnInfo[nrKeyCols];
    ciKey[0] =
        new ColumnInfo( BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.ColumnInfo.TableField" ),
            ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    ciKey[1] =
        new ColumnInfo(
            BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.ColumnInfo.Comparator" ), ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "=", "= ~NULL", "<>", "<", "<=", //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
              ">", ">=", "LIKE", "IS NULL", "IS NOT NULL" } ); //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
    ciKey[2] =
        new ColumnInfo( BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.ColumnInfo.StreamField" ),
            ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    tableFieldColumns.add( ciKey[0] );
    final TableView wKey =
        new TableView( transMeta, cGenScriptFields, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL
            | SWT.H_SCROLL, ciKey, nrKeyRows, lsMod, props );

    final Button wGet = new Button( cGenScriptFields, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.GetFields.Button" ) );
    wGet.addListener( SWT.Selection, new Listener() {
      @Override
      public void handleEvent( Event e ) {
        setTableFieldCombo.run();
        try {
          RowMetaInterface r = transMeta.getPrevStepFields( stepname );
          if ( r != null ) {
            TableItemInsertListener listener = new TableItemInsertListener() {
              @Override
              public boolean tableItemInserted( TableItem tableItem, ValueMetaInterface v ) {
                tableItem.setText( 2, "=" );
                return true;
              }
            };
            BaseStepDialog.getFieldsFromPrevious( r, wKey, 1, new int[] { 1, 3 }, new int[] {}, -1, -1, listener );
          }
        } catch ( KettleException ke ) {
          new ErrorDialog( shell, BaseMessages.getString( PKG, "InsertUpdateDialog.FailedToGetFields.DialogTitle" ),
              BaseMessages.getString( PKG, "InsertUpdateDialog.FailedToGetFields.DialogMessage" ), ke );
        }
      }
    } );

    FormData fdGet = new FormData();
    fdGet.right = new FormAttachment( 100, 0 );
    fdGet.top = new FormAttachment( wlKey, margin );
    wGet.setLayoutData( fdGet );

    FormData fdKey = new FormData();
    fdKey.left = new FormAttachment( 0, 0 );
    fdKey.top = new FormAttachment( wlKey, margin );
    fdKey.right = new FormAttachment( wGet, -margin );
    // fdKey.bottom = new FormAttachment(wlKey, 190);
    wKey.setLayoutData( fdKey );

    // The field Table
    Label wlReturn = new Label( cGenScriptFields, SWT.NONE );
    wlReturn.setText( BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.Fields.Label" ) );
    props.setLook( wlReturn );
    FormData fdlReturn = new FormData();
    fdlReturn.left = new FormAttachment( 0, 0 );
    fdlReturn.top = new FormAttachment( wKey, margin );
    wlReturn.setLayoutData( fdlReturn );

    int UpInsCols = 3;
    int UpInsRows = ( input.getFieldTable() != null ? input.getFieldTable().length : 1 );

    ColumnInfo[] ciReturn = new ColumnInfo[UpInsCols];
    ciReturn[0] =
        new ColumnInfo( BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.ColumnInfo.TableField" ),
            ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    ciReturn[1] =
        new ColumnInfo( BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.ColumnInfo.StreamField" ),
            ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    ciReturn[2] =
        new ColumnInfo( BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.ColumnInfo.UpdateField" ),
            ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "Y", "N" } );

    tableFieldColumns.add( ciReturn[0] );
    final TableView wReturn =
        new TableView( transMeta, cGenScriptFields, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL
            | SWT.H_SCROLL, ciReturn, UpInsRows, lsMod, props );

    Button wGetMapping = new Button( cGenScriptFields, SWT.PUSH );
    wGetMapping.setText( BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.GetFields.Label" ) );
    wGetMapping.addListener( SWT.Selection, new Listener() {
      @Override
      public void handleEvent( Event e ) {
        try {
          setTableFieldCombo.run();
          RowMetaInterface r = transMeta.getPrevStepFields( stepname );
          if ( r != null ) {
            TableItemInsertListener listener = new TableItemInsertListener() {
              @Override
              public boolean tableItemInserted( TableItem tableItem, ValueMetaInterface v ) {
                if ( v.getType() == ValueMetaInterface.TYPE_DATE ) {
                  // The default is : format is OK for dates, see if this sticks later on...
                  //
                  tableItem.setText( 3, "Y" );
                } else {
                  tableItem.setText( 3, "Y" ); // default is OK too...
                }
                return true;
              }
            };
            BaseStepDialog.getFieldsFromPrevious( r, wReturn, 1, new int[] { 1, 2 }, new int[] {}, -1, -1, listener );
          }
        } catch ( KettleException ke ) {
          new ErrorDialog( shell, BaseMessages
              .getString( PKG, "TeraDataBulkLoaderDialog.FailedToGetFields.DialogTitle" ), BaseMessages.getString( PKG,
                "TeraDataBulkLoaderDialog.FailedToGetFields.DialogMessage" ), ke );
        }
      }
    } );
    FormData fdGetLU = new FormData();
    fdGetLU.top = new FormAttachment( wlReturn, margin );
    fdGetLU.right = new FormAttachment( 100, 0 );
    wGetMapping.setLayoutData( fdGetLU );

    Button wDoMapping = new Button( cGenScriptFields, SWT.PUSH );
    wDoMapping.setText( BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.EditMapping.Label" ) );
    FormData fdDoMapping = new FormData();
    fdDoMapping.top = new FormAttachment( wGetMapping, margin );
    fdDoMapping.right = new FormAttachment( 100, 0 );
    wDoMapping.setLayoutData( fdDoMapping );
    wDoMapping.addListener( SWT.Selection, new Listener() {
      @Override
      public void handleEvent( Event arg0 ) {
        generateMappings( wReturn, wSchema, wTable );
      }
    } );

    FormData fdReturn = new FormData();
    fdReturn.left = new FormAttachment( 0, 0 );
    fdReturn.top = new FormAttachment( wlReturn, margin );
    fdReturn.right = new FormAttachment( wGetMapping, -margin );
    fdReturn.bottom = new FormAttachment( 100, 0 );
    wReturn.setLayoutData( fdReturn );

    final Runnable disableKeytable = new Runnable() {

      @Override
      public void run() {
        int choice = cActionType.getSelection();
        if ( choice == 0 ) {
          wKey.setEnabled( false );
          wKey.setVisible( false );
          wGet.setEnabled( false );
          wGet.setVisible( false );
          wlKey.setVisible( false );
        } else {
          wKey.setEnabled( true );
          wKey.setVisible( true );
          wGet.setEnabled( true );
          wGet.setVisible( true );
          wlKey.setVisible( true );
        }
      }
    };
    cActionType.setCallback( disableKeytable );

    dialogPopulators.add( new DialogPopulator() {

      @Override
      public void validate( List<String> errors ) {
      }

      @Override
      public void populateMeta( TeraDataBulkLoaderMeta inf ) {
        int nrkeys = wKey.nrNonEmpty();
        int nrfields = wReturn.nrNonEmpty();
        inf.allocate( nrkeys, nrfields );
        if ( log.isDebug() ) {
          logDebug( BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.Log.FoundFields", "" + nrfields ) );
        }
        for ( int i = 0; i < nrkeys; i++ ) {
          TableItem item = wKey.getNonEmpty( i );
          ( inf.getKeyLookup() )[i] = item.getText( 1 );
          ( inf.getKeyCondition() )[i] = item.getText( 2 );
          ( inf.getKeyStream() )[i] = item.getText( 3 );
        }

        for ( int i = 0; i < nrfields; i++ ) {
          TableItem item = wReturn.getNonEmpty( i );
          ( inf.getFieldTable() )[i] = item.getText( 1 );
          ( inf.getFieldStream() )[i] = item.getText( 2 );
          ( inf.getFieldUpdate() )[i] = Boolean.valueOf( "Y".equals( item.getText( 3 ) ) );
        }
        inf.setActionType( cActionType.getSelection() );
      }

      @Override
      public void populateDialog( TeraDataBulkLoaderMeta input ) {
        int ival;
        if ( ( ival = input.getActionType() ) >= 0 ) {
          cActionType.setSelection( ival );
        }
        if ( log.isDebug() ) {
          logDebug( BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.Log.GettingKeyInfo" ) );
        }
        if ( input.getKeyStream() != null ) {
          for ( int i = 0; i < input.getKeyStream().length; i++ ) {
            TableItem item = wKey.table.getItem( i );
            if ( input.getKeyLookup()[i] != null ) {
              item.setText( 1, input.getKeyLookup()[i] );
            }
            if ( input.getKeyCondition()[i] != null ) {
              item.setText( 2, input.getKeyCondition()[i] );
            }
            if ( input.getKeyStream()[i] != null ) {
              item.setText( 3, input.getKeyStream()[i] );
            }
          }
        }
        if ( input.getFieldTable() != null ) {
          logDebug( BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.FieldTableLength",
              input.getFieldTable().length ) );
          for ( int i = 0; i < input.getFieldTable().length; i++ ) {
            TableItem item = wReturn.table.getItem( i );
            if ( input.getFieldTable()[i] != null ) {
              item.setText( 1, input.getFieldTable()[i] );
            }
            if ( input.getFieldStream()[i] != null ) {
              item.setText( 2, input.getFieldStream()[i] );
            }
            if ( input.getFieldUpdate()[i] == null || input.getFieldUpdate()[i].booleanValue() ) {
              item.setText( 3, "Y" );
            } else {
              item.setText( 3, "N" );
            }
            logDebug( item.toString() );
          }
        }
        wReturn.setRowNums();
        wReturn.optWidth( true );
      }
    } );

    cGenScriptFields.setLayout( new FormLayout() );
    tiGenScriptFields.setControl( cGenScriptFields );

    List<String> fields = new ArrayList<String>( getInputFields().getInputFields().keySet() );
    Collections.sort( fields );
    ciKey[2].setComboValues( fields.toArray( new String[fields.size()] ) );
    ciReturn[1].setComboValues( fields.toArray( new String[fields.size()] ) );
    disableKeytable.run();
    setTableFieldCombo.run();
  }

  private void createUseScriptItemsTab( final GeneralTabWidgets generalTabWidgets ) {
    final CTabItem tiUseScriptItems = new CTabItem( fItemSet, SWT.NONE );
    tiUseScriptItems.setText( BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.ScriptTab.Label" ) );
    Composite cUseScriptItems = new Composite( fItemSet, SWT.BORDER );

    /******************************************************************************************************************/
    /********************************************************* Use Script Options Group *******************************/
    /******************************************************************************************************************/
    final TextVarMenuItem wControlFile =
        new FileTextVarMenuItem( shell, cUseScriptItems, props, transMeta, lsMod, (Control) null,
            "TeraDataBulkLoaderDialog.ControlFile.Label" );

    CompositeMenuItem wSubstituteControlFile =
        new CompositeMenuItem( props, lsMod, lsModSelect, input, cUseScriptItems, wControlFile,
            "TeraDataBulkLoaderDialog.SubstituteControlFile.Label", 0 );
    final Button wbSubstituteControlFile = wSubstituteControlFile.addButton( "", SWT.CHECK );
    wSubstituteControlFile.addLabel( "TeraDataBulkLoaderDialog.DataFileRandomizeVariable.Label" ).setForeground(
        GUIResource.getInstance().getColorOrange() );

    final TextVarMenuItem wVariableFile =
        new FileTextVarMenuItem( shell, cUseScriptItems, props, transMeta, lsMod, wSubstituteControlFile,
            "TeraDataBulkLoaderDialog.VariableFile.Label" );

    /***************************************************/

    tiUseScriptItems.setControl( cUseScriptItems );

    cUseScriptItems.setLayout( new FormLayout() );
    props.setLook( cUseScriptItems );

    FormData fdScript = new FormData();
    fdScript.left = new FormAttachment( 0, margin );
    fdScript.top = new FormAttachment( 0, margin );
    fdScript.right = new FormAttachment( 100, -margin );
    fdScript.bottom = new FormAttachment( 100, -margin );
    cUseScriptItems.setLayoutData( fdScript );
    dialogPopulators.add( new DialogPopulator() {

      @Override
      public void validate( List<String> errors ) {
        addIfTrue( errors, Const.isEmpty( wControlFile.getText() ),
            "TeraDataBulkLoaderDialog.MissingControlFile.DialogMessage" );
        boolean fifoBadState =
            generalTabWidgets.getRandomizeFifoButton().getSelection() && !wbSubstituteControlFile.getSelection();
        addIfTrue( errors, fifoBadState, "TeraDataBulkLoaderDialog.FIFOState.DialogMessage" );
        if ( fifoBadState ) {
          fItemSet.setSelection( tiUseScriptItems );
        }
      }

      @Override
      public void populateMeta( TeraDataBulkLoaderMeta inf ) {
        inf.setExistingScriptFile( wControlFile.getText() );
        inf.setSubstituteControlFile( wbSubstituteControlFile.getSelection() );
        inf.setVariableFile( wVariableFile.getText() );
      }

      @Override
      public void populateDialog( TeraDataBulkLoaderMeta input ) {
        String val;
        if ( ( val = input.getExistingScriptFile() ) != null ) {
          wControlFile.setText( val );
        }
        wbSubstituteControlFile.setSelection( input.getSubstituteControlFile() );
        if ( ( val = input.getVariableFile() ) != null ) {
          wVariableFile.setText( val );
        }
      }
    } );
  }

  public void createDynamicTabs() {
    int thisSelection = cScriptOption.getSelection();
    if ( lastTypeSelection != thisSelection ) {
      if ( thisSelection == 0 ) {
        // enable disable as appropriate
        wPreviewScript.setEnabled( true );
        createDynamicTabs( true );
      } else {
        // enable disable as appropriate
        wPreviewScript.setEnabled( false );
        createDynamicTabs( false );
      }
      getDynamicData();
    }
    lastTypeSelection = thisSelection;
  }

  public void createDynamicTabs( boolean generated ) {
    dialogPopulators.clear();
    int i = 0;
    while ( ( i = fItemSet.getItemCount() - 1 ) >= 0 ) {
      CTabItem tab = fItemSet.getItem( i );
      tab.getControl().dispose();
      tab.dispose();
    }
    GeneralTabWidgets tiExecutionItems = createExecutionTab();
    if ( generated ) {
      createGenScriptTabs();
    } else {
      createUseScriptItemsTab( tiExecutionItems );
    }
    fItemSet.setSelection( tiExecutionItems.getTab() );
  }

  /**
   * Reads in the fields from the previous steps and from the ONE next step and opens an EnterMappingDialog with this
   * information. After the user did the mapping, those information is put into the Select/Rename table.
   */
  private void generateMappings( TableView wReturn, TextVarMenuItem wSchema, TextVarMenuItem wTable ) {

    // Determine the source and target fields...
    //
    RowMetaInterface sourceFields;
    RowMetaInterface targetFields;

    try {
      sourceFields = transMeta.getPrevStepFields( stepMeta );
    } catch ( KettleException e ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG,
          "TeraDataBulkLoaderDialog.DoMapping.UnableToFindSourceFields.Title" ), BaseMessages.getString( PKG,
            "TeraDataBulkLoaderDialog.DoMapping.UnableToFindSourceFields.Message" ), e );
      return;
    }
    // refresh data
    input.setDatabaseMeta( transMeta.findDatabase( wConnection.getText() ) );
    input.setTableName( transMeta.environmentSubstitute( wTable.getText() ) );
    input.setSchemaName( transMeta.environmentSubstitute( wSchema.getText() ) );
    StepMetaInterface stepMetaInterface = stepMeta.getStepMetaInterface();
    try {
      targetFields = stepMetaInterface.getRequiredFields( transMeta );
    } catch ( KettleException e ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG,
          "TeraDataBulkLoaderDialog.DoMapping.UnableToFindTargetFields.Title" ), BaseMessages.getString( PKG,
            "TeraDataBulkLoaderDialog.DoMapping.UnableToFindTargetFields.Message" ), e );
      return;
    }

    String[] inputNames = new String[sourceFields.size()];
    for ( int i = 0; i < sourceFields.size(); i++ ) {
      ValueMetaInterface value = sourceFields.getValueMeta( i );
      inputNames[i] = value.getName() + EnterMappingDialog.STRING_ORIGIN_SEPARATOR + value.getOrigin() + ")";
    }

    // Create the existing mapping list...
    //
    List<SourceToTargetMapping> mappings = new ArrayList<SourceToTargetMapping>();
    StringBuffer missingSourceFields = new StringBuffer();
    StringBuffer missingTargetFields = new StringBuffer();

    int nrFields = wReturn.nrNonEmpty();
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wReturn.getNonEmpty( i );
      String source = item.getText( 2 );
      String target = item.getText( 1 );

      int sourceIndex = sourceFields.indexOfValue( source );
      if ( sourceIndex < 0 ) {
        missingSourceFields.append( Const.CR + "   " + source + " --> " + target );
      }
      int targetIndex = targetFields.indexOfValue( target );
      if ( targetIndex < 0 ) {
        missingTargetFields.append( Const.CR + "   " + source + " --> " + target );
      }
      if ( sourceIndex < 0 || targetIndex < 0 ) {
        continue;
      }

      SourceToTargetMapping mapping = new SourceToTargetMapping( sourceIndex, targetIndex );
      mappings.add( mapping );
    }

    // show a confirm dialog if some missing field was found
    //
    if ( missingSourceFields.length() > 0 || missingTargetFields.length() > 0 ) {

      String message = "";
      if ( missingSourceFields.length() > 0 ) {
        message +=
            BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.DoMapping.SomeSourceFieldsNotFound",
                missingSourceFields.toString() )
                + Const.CR;
      }
      if ( missingTargetFields.length() > 0 ) {
        message +=
            BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.DoMapping.SomeTargetFieldsNotFound",
                missingSourceFields.toString() )
                + Const.CR;
      }
      message += Const.CR;
      message +=
          BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.DoMapping.SomeFieldsNotFoundContinue" ) + Const.CR;
      MessageDialog.setDefaultImage( GUIResource.getInstance().getImageSpoon() );
      boolean goOn =
          MessageDialog.openConfirm( shell, BaseMessages.getString( PKG,
              "TeraDataBulkLoaderDialog.DoMapping.SomeFieldsNotFoundTitle" ), message );
      if ( !goOn ) {
        return;
      }
    }
    EnterMappingDialog d =
        new EnterMappingDialog( TeraDataBulkLoaderDialog.this.shell, sourceFields.getFieldNames(), targetFields
            .getFieldNames(), mappings );
    mappings = d.open();

    // mappings == null if the user pressed cancel
    //
    if ( mappings != null ) {
      // Clear and re-populate!
      //
      wReturn.table.removeAll();
      wReturn.table.setItemCount( mappings.size() );
      for ( int i = 0; i < mappings.size(); i++ ) {
        SourceToTargetMapping mapping = (SourceToTargetMapping) mappings.get( i );
        TableItem item = wReturn.table.getItem( i );
        item.setText( 2, sourceFields.getValueMeta( mapping.getSourcePosition() ).getName() );
        item.setText( 1, targetFields.getValueMeta( mapping.getTargetPosition() ).getName() );
      }
      wReturn.setRowNums();
      wReturn.optWidth( true );
    }
  }

  /**
   * Gets the stream and table fields.
   * 
   * @return the stream and table fields
   */
  public String[] getStreamAndTableFields() {
    List<String> list = new ArrayList<String>();

    if ( input.getFieldTable() != null ) {
      for ( int i = 0; i < input.getFieldTable().length; i++ ) {
        if ( ( input.getFieldTable()[i] != null ) && ( input.getFieldStream()[i] != null ) ) {
          list.add( input.getFieldTable()[i] + " = :" + input.getFieldStream()[i] );
        }
      }
    }

    String[] simpleArray = new String[list.size()];
    return list.toArray( simpleArray );
  }

  /**
   * Sets the table field combo.
   */
  private void setTableFieldCombo( final TextVarMenuItem wSchema, final TextVarMenuItem wTable ) {
    if ( !wTable.getTextVar().isDisposed() && !wConnection.isDisposed() ) {
      // clear
      for ( int i = 0; i < tableFieldColumns.size(); i++ ) {
        ColumnInfo colInfo = tableFieldColumns.get( i );
        colInfo.setComboValues( new String[] {} );
      }
      if ( !Const.isEmpty( wTable.getText() ) ) {
        DatabaseMeta ci = transMeta.findDatabase( wConnection.getText() );
        if ( ci != null ) {
          Database db = new Database( loggingObject, ci );
          try {
            db.connect();

            String schemaTable =
                ci.getQuotedSchemaTableCombination( transMeta.environmentSubstitute( wSchema.getText() ), transMeta
                    .environmentSubstitute( wTable.getText() ) );
            RowMetaInterface r = db.getTableFields( schemaTable );
            if ( null != r ) {
              String[] fieldNames = r.getFieldNames();
              if ( null != fieldNames ) {
                for ( int i = 0; i < tableFieldColumns.size(); i++ ) {
                  ColumnInfo colInfo = tableFieldColumns.get( i );
                  colInfo.setComboValues( fieldNames );
                }
              }
            }
          } catch ( Exception e ) {
            for ( int i = 0; i < tableFieldColumns.size(); i++ ) {
              ColumnInfo colInfo = tableFieldColumns.get( i );
              colInfo.setComboValues( new String[] {} );
            }
            // ignore any errors here. drop downs will not be
            // filled, but no problem for the user
          }
        }
      }
    }
  }

  /**
   * Cancel.
   */
  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  /**
   * Gets the info.
   * 
   * @param inf
   *          the inf
   * @return the info
   */
  private void getInfo( TeraDataBulkLoaderMeta inf ) {
    // populate meta from widgets
    for ( DialogPopulator dialogPopulator : dialogPopulators ) {
      dialogPopulator.populateMeta( inf );
    }

    inf.setGenerateScript( cScriptOption.getSelection() == 0 );
    inf.setDatabaseMeta( transMeta.findDatabase( wConnection.getText() ) );
    stepname = wStepname.getText(); // return value
  }

  private <T> void addIfTrue( List<String> list, boolean condition, String message ) {
    if ( condition ) {
      list.add( BaseMessages.getString( PKG, message ) );
    }
  }

  /**
   * Ok.
   */
  private void ok() {
    final RequiredFieldsError errorPopup =
        new RequiredFieldsError( shell, BaseMessages.getString( PKG,
            "TeraDataBulkLoaderDialog.MissingRequiredTitle.DialogMessage" ), BaseMessages.getString( PKG,
              "TeraDataBulkLoaderDialog.MissingRequiredMsg.DialogMessage" ) );

    // Always required
    errorPopup.addIfUndef( wStepname, "TeraDataBulkLoaderDialog.MissingStepname.DialogMessage" );
    errorPopup.addIfUndef( wConnection, "TeraDataBulkLoaderDialog.InvalidConnection.DialogMessage" );

    // Required depending on option
    List<String> errors = new ArrayList<String>();
    for ( DialogPopulator dialogPopulator : dialogPopulators ) {
      dialogPopulator.validate( errors );
    }
    for ( String error : errors ) {
      errorPopup.addMessage( error );
    }

    // check for errors?
    if ( errorPopup.hasErrors() ) {
      errorPopup.display();
    } else {
      getInfo( input );
      dispose();
    }
  }

  public void getData() {
    cScriptOption.setSelection( input.getGenerateScript() ? 0 : 1 );

    if ( input.getDatabaseMeta() != null ) {
      wConnection.setText( input.getDatabaseMeta().getName() );
    } else {
      if ( transMeta.nrDatabases() == 1 ) {
        wConnection.setText( transMeta.getDatabase( 0 ).getName() );
      }
    }

    createDynamicTabs();

    wStepname.selectAll();
    wStepname.setFocus();
  }

  public void getDynamicData() {
    for ( DialogPopulator dialogPopulator : dialogPopulators ) {
      dialogPopulator.populateDialog( input );
    }
  }

  /**
   * Preview script.
   */
  public void previewScript() {
    InputFields inputFields = getInputFields();
    TeraDataBulkLoaderMeta metacopy = new TeraDataBulkLoaderMeta();
    getInfo( metacopy );
    TeraDataBulkLoaderRoutines routines = new TeraDataBulkLoaderRoutines( null, metacopy );
    try {
      String script =
          routines.createGeneratedScriptFile( inputFields.getInputFieldType(), inputFields.getInputFieldLength() );
      Shell mb = new Shell();
      mb.setLayout( new FillLayout() );
      Text msg = new Text( mb, SWT.BORDER_SOLID | SWT.MULTI | SWT.V_SCROLL );
      mb.setText( BaseMessages.getString( PKG, "TeraDataBulkLoaderDialog.Preview.MessageBoxTitle" ) );
      msg.setText( script );
      msg.setEditable( false );
      mb.open();
    } catch ( Exception e ) {
      e.printStackTrace();
    }
  }
}
