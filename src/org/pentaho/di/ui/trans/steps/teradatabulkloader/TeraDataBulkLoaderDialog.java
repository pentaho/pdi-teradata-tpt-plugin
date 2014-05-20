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

package org.pentaho.di.ui.trans.steps.teradatabulkloader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
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
import org.pentaho.di.ui.core.database.dialog.DatabaseExplorerDialog;
import org.pentaho.di.ui.core.dialog.EnterMappingDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.trans.step.TableItemInsertListener;


/**
 * Dialog class for the TeraData bulk loader step.
 * 
 */
public class TeraDataBulkLoaderDialog extends BaseStepDialog implements StepDialogInterface
{
	private static Class<?> PKG = TeraDataBulkLoaderMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
	
	static final int NO_BUTTON    = 0;
	static final int FILE_BUTTON  = 1;
	static final int DIR_BUTTON   = 2;
	static final int TABLE_BUTTON = 4;
	static final int LABEL_ONLY   = 8;
	static final int NO_VAR       = 16;

	/*  The "useful" widgets are listed first for clarity.  Associated labels, buttons form data follow......   */
	/*************************** These variables are common to both generated and existing scripts ***************/
	private CCombo				wConnection;

	CTabFolder fItemSet = null; 
    private CTabItem tiExecutionItems = null;
    private CTabItem tiUseScriptItems = null;
    private CTabItem tiGenScriptControlItems = null;
    private CTabItem tiGenScriptFields = null;
    
    private Composite cExecutionItems;
    private Composite cGenScriptControlItems;
    private Composite cGenScriptFields;
    private Composite cUseScriptItems;
    private TextVarMenuItem     cExecutionItemsDisableMsg;
    private TextVarMenuItem     cGenScriptControlItemsDisableMsg;
    private TextVarMenuItem     cGenScriptFieldsDisableMsg;
    private TextVarMenuItem     cUseScriptItemsDisableMsg;


	/**** these go into the "Execution Environment" tab */
	private TextVarMenuItem		wTbuildPath;                         // (tbuildPath)
	private TextVarMenuItem     wDataFile;                           // (fifoFileName)
	private TextVarMenuItem     wJobName;                            //
	private TextVarMenuItem		wTwbRoot;
	private TextVarMenuItem		wTdInstall;
	private TextVarMenuItem		wTbuildLibPath;
	private TextVarMenuItem		wTdicuLibPath;
	private TextVarMenuItem		wLibPath;
	private TextVarMenuItem		wCopLibPath;
	
	/* the remainder go into "Script Configuration" tab */
	private RadioComposite      cScriptOption;                       // (generateScript) - single boolean variable

	
	/*************************** These variables are for using a pre-existing control file ***************/
	private TextVarMenuItem     wControlFile, wVariableFile;                           // (controlFile, variableFile) 
	private Button              wbSubstituteControlFile;     // (substituteControlFile, substituteVariableFile)
	
	
	/*************************** These variables are for using a generated control file ***************/
	private TextVarMenuItem             wSchema;                             // (schemaName)
	private TextVarMenuItem				wTable;                              // (tableName)  target table
	private TextVarMenuItem				wLogTable;                           // (logTable)
	private TextVarMenuItem				wWorkTable;                          // (workTable)
	private TextVarMenuItem				wErrorTable;                         // (errorTable)
	private TextVarMenuItem				wErrorTable2;                        // (errorTable2)
	private Button              wbDropError, wbDropError2, wbDropLog, wbDropWork;  // (dropErrorTable, dropErrorTable2, dropLogTable, dropWorkTable)
	private Button              wbIgnoreDupUpdate, wbInsertMissingUpdate, wbIgnoreMissingUpdate;  // (ignoreDupUpdate, insertMissingUpdate, ignoreMissingUpdate)
	private TextVarMenuItem				wAccessLogFile;                      // (accessLogFile)
	private TextVarMenuItem				wUpdateLogFile;                      // (updateLogFile)
	private TextVarMenuItem             wScriptFile;                         // (scriptFileName) filename of generated script
	private RadioComposite      cActionType;                         // (actionType)
	private TableView           wKey, wReturn;
	private Button              wPreviewScript;

	
	
	CompositeMenuItem wbControlFile;

	private TeraDataBulkLoaderMeta	input;
	
	
	/************************************  associated widgets for generated script *****************/
	private Label               wlKey, wlReturn;
	private FormData            fdlReturn, fdReturn;
	private Button				wGetMapping;
	private FormData			fdGetLU;
	private Listener			lsGetLU;
	private Button              wDoMapping;
	private FormData            fdDoMapping;
	private ColumnInfo[]        ciKey, ciReturn ;

	private Map<String, Integer> inputFields;
	private Map<String, Integer> inputFieldType;
	private Map<String, Integer> inputFieldLength;
	/**
	 * List of ColumnInfo that should have the field names of the selected database table
	 */
	private List<ColumnInfo> tableFieldColumns = new ArrayList<ColumnInfo>();

	private class TableSelectionButtonListener extends SelectionAdapter{
		TextVar textWidget;
		TableSelectionButtonListener(TextVar textWidget){
			super();
			this.textWidget = textWidget;
		}
		
		public void widgetSelected(SelectionEvent e){
			DatabaseMeta inf = null;
			// New class: SelectTableDialog
			int connr = wConnection.getSelectionIndex();
			if (connr >= 0)
				inf = transMeta.getDatabase(connr);

			if (inf != null)
			{
				if(log.isDebug()) logDebug(BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.Log.LookingAtConnection") + inf.toString()); 

				DatabaseExplorerDialog std = new DatabaseExplorerDialog(shell, SWT.NONE, inf, transMeta.getDatabases());
				std.setSelectedSchemaAndTable(wSchema.getText(), wTable.getText());
				if (std.open())
				{
	                wSchema.setText(Const.NVL(std.getSchemaName(), ""));
	                this.textWidget.setText(Const.NVL(std.getTableName(), ""));
	            	setTableFieldCombo();
				}
			}
			else
			{
				MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
				mb.setMessage(BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.InvalidConnection.DialogMessage")); 
				mb.setText(BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.InvalidConnection.DialogTitle")); 
				mb.open();
			}
		}
	}
	
	
	private class FileSelectionButtonListener extends SelectionAdapter{
		TextVar textWidget;
		FileSelectionButtonListener(TextVar textWidget){
			super();
			this.textWidget = textWidget;
		}
		
		public void widgetSelected(SelectionEvent e) {
			FileDialog dialog = new FileDialog(shell, SWT.OPEN);
			if (dialog.open()!=null)
			{
				String str = dialog.getFilterPath()+System.getProperty("file.separator")+dialog.getFileName();
				this.textWidget.setText(str);
			}
		}
	}
	
	
	private class DirectorySelectionButtonListener extends SelectionAdapter{
		TextVar textWidget;
		DirectorySelectionButtonListener(TextVar textWidget){
			super();
			this.textWidget = textWidget;
		}
		
		public void widgetSelected(SelectionEvent e) {
			DirectoryDialog dialog = new DirectoryDialog(shell, SWT.OPEN);
			if (dialog.open()!=null)
			{
				String str = dialog.getFilterPath()+System.getProperty("file.separator")+dialog.getText();
				this.textWidget.setText(str);
			}
		}
	}
	
	
	private class RadioComposite extends Composite{

		private Listener lsActionType;
		private Listener lsPassedListener = null;
		
		RadioComposite(Composite parent, int opts) {
			super(parent, opts);
			lsActionType = new Listener () {
				public void handleEvent (Event event) {
					Control [] children = getChildren ();
					for (int j=0; j<children.length; j++) {
						Control child = children [j];
						if (child instanceof Button) {
							Button bc = (Button) child;
							bc.setSelection (false);
						}
					}
					Button button = (Button) event.widget;
					button.setSelection (true);
					input.setChanged();
				}
			};
		}
		
		public void addListener(Listener ls){
			this.lsPassedListener = ls;
		}
		
		public void addButtons(String[] labels){
			for (int i=0; i < labels.length; i++){
				this.addButton(labels[i]);
			}
		}
		
		public void addButton(String label){
			Button button = new Button(this, SWT.RADIO | SWT.RIGHT | SWT.BORDER);
			button.setText(label);
			props.setLook(button);
			button.addListener (SWT.Selection, lsActionType);
			if (lsPassedListener != null){
				button.addListener(SWT.Selection,lsPassedListener);
			}
		}
		
		public int getSelection(){
			Control [] children = getChildren ();
			for (int j=0; j<children.length; j++) {
				Control child = children [j];
				if (child instanceof Button) {
					Button bc = (Button) child;
					if (bc.getSelection ()){
						return j;
					}
				}
			}
			return 0;
		}
		
		
		public void setSelection(int index){
			Control [] children = getChildren ();
			for (int j=0; j<children.length; j++) {
				Control child = children [j];
				if (child instanceof Button) {
					Button bc = (Button) child;
					if (j == index){
						bc.setSelection (true);
					}else{
						bc.setSelection (false);
					}
				}
			}
			notifyListeners( SWT.SELECTED, new Event() );
		}
	}

	private class CompositeMenuItem {
		private Label label;
		private Composite composite;
		private List<Control> items = new ArrayList<Control>();
		int margin = Const.MARGIN;
		int middle = props.getMiddlePct();
		
		CompositeMenuItem(Composite parent, TextVarMenuItem top, String labelProp, int type){
			this(parent,top.getButton(),labelProp,type);
		}
		
		CompositeMenuItem(Composite parent, Control top, String labelProp, int type){
			label = new Label(parent, SWT.RIGHT);
			label.setText(BaseMessages.getString(PKG, labelProp)); 
			FormData fdl = new FormData();
			fdl.left  = new FormAttachment(0, margin);
			if (top == null){
				fdl.top   = new FormAttachment(0, margin);
			}else{
				fdl.top   = new FormAttachment(top, margin);
			}
			fdl.right = new FormAttachment(middle, -margin);
			props.setLook(label);
			label.setLayoutData(fdl);
			
			
			composite = new Composite(parent, SWT.NONE);
			composite.setLayout (new FormLayout ());

			FormData fdc = new FormData();
			fdc.left  = new FormAttachment(label, 0);
			if (top == null){
				fdc.top   = new FormAttachment(0, 0);
			}else{
				fdc.top   = new FormAttachment(top, 0);
			}
			fdc.right = new FormAttachment(100, 0);
			composite.setLayoutData(fdc);
			props.setLook( composite );
		}

		public Composite getComposite() {
			return composite;
		}

		public void add(Control item) {
			FormData fd = new FormData();
			fd.top   = new FormAttachment(0, margin);
			if (items.size() > 0){
				fd.left = new FormAttachment(items.get(items.size() - 1), margin);
			}else{
				fd.left = new FormAttachment(0,margin);
			}
			item.setLayoutData(fd);
			props.setLook( item );
			items.add(item); 
		}

		@SuppressWarnings("unused")
		public CCombo addCCombo() {
			CCombo combo = new CCombo(composite, SWT.BORDER | SWT.READ_ONLY);
			combo.setEditable(true);
	        props.setLook(combo);
	        combo.addModifyListener(lsMod);
	        add(combo);
			return combo;
		}
		
		public Button addButton(String label, int radio){
			Button button = new Button(composite, radio | SWT.RIGHT | SWT.BORDER);
			if (! Const.isEmpty(label)){
				button.setText(BaseMessages.getString(PKG, label));
			}
			button.addListener(SWT.CHECK, lsModSelect);
			props.setLook(button);
			add(button);
			return button;
		}
		
		public RadioComposite addRadioComposite(String[] labels, int opts, final Object p, final Runnable callback){
			RadioComposite rc = new RadioComposite(composite, opts);
			rc.setLayout (new RowLayout ());
			if (callback != null){
				rc.addListener(new Listener() {
					public void handleEvent (Event event) {
						callback.run();
					}
				});
			}
			rc.addButtons(labels);
			add(rc);
			return rc;
		}

		@SuppressWarnings("unused")
		public RadioComposite addRadioComposite(String[] actionTypes, int i) {
			return addRadioComposite(actionTypes,i,null,null);
		}

		@SuppressWarnings("unused")
		public void setVisible(boolean b) {
			label.setVisible(b);
			composite.setVisible(b);
		}
		
	}
	
	
	private class TextVarMenuItem {
		private Label label;
		private Button button;
		private TextVar textvar;
		private Text    text;
		private int type;
		
		int margin = Const.MARGIN;
		int middle = props.getMiddlePct();

		TextVarMenuItem(Composite parent, TextVarMenuItem top, String labelProp, int buttonType){
			this(parent,top.getButton(),labelProp,buttonType);
		}
		TextVarMenuItem(Composite parent, CompositeMenuItem top, String labelProp, int buttonType){
			this(parent,top.getComposite(),labelProp,buttonType);
		}
		TextVarMenuItem(Composite parent, Control top, String labelProp, int buttonType){
			type = buttonType;



			button = new Button(parent, SWT.PUSH | SWT.CENTER);
			button.setText(BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.Browse.Button"));
			props.setLook(button);
			FormData fdb = new FormData();
			if (top == null){
				fdb.top   = new FormAttachment(0, margin);
			}else{
				fdb.top   = new FormAttachment(top, margin);
			}
			fdb.right = new FormAttachment(100, -margin);
			button.setLayoutData(fdb);
			if (type == NO_BUTTON || type == NO_VAR  || type == LABEL_ONLY){
				button.setVisible(false);
				button.dispose();
				button = null;
			}

			
			label = new Label(parent, SWT.RIGHT);
			label.setText(BaseMessages.getString(PKG, labelProp)); 
			FormData fdl = new FormData();
			fdl.left  = new FormAttachment(0, margin);
			if (top == null){
				fdl.top   = new FormAttachment(0, margin);
			}else{
				fdl.top   = new FormAttachment(top, margin);
			}
			if ((type & LABEL_ONLY) != 0){
				fdl.right = new FormAttachment(100, -margin);
				label.setLayoutData(fdl);
				label.setAlignment(SWT.CENTER);
				return;
			}
			fdl.right = new FormAttachment(middle, -margin);
			props.setLook(label);
			label.setLayoutData(fdl);
			

			FormData fdt = new FormData();
			fdt.left = new FormAttachment(middle, margin);
			if (top == null){
				fdt.top = new FormAttachment(0, margin);
			}else{
				fdt.top = new FormAttachment(top, margin);
			}
			if (type == NO_BUTTON || type == NO_VAR  || type == LABEL_ONLY){
				fdt.right = new FormAttachment(100, -margin);
			}else{
				fdt.right = new FormAttachment(button, -margin);
			}
			if (type == NO_VAR){
				text = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
				props.setLook(text);
				text.addModifyListener(lsMod);
				text.setLayoutData(fdt);
			}else{
				textvar = new TextVar(transMeta, parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
				props.setLook(textvar);
				textvar.addModifyListener(lsMod);
				textvar.setLayoutData(fdt);
			}

			switch (type){
			case FILE_BUTTON:  button.addSelectionListener(new FileSelectionButtonListener(textvar)); break;
			case DIR_BUTTON:   button.addSelectionListener(new DirectorySelectionButtonListener(textvar)); break;
			case TABLE_BUTTON: button.addSelectionListener(new TableSelectionButtonListener(textvar)); break;
			}
		}

		public Control 	getButton()			{ return button != null ? button : (textvar != null ? textvar : label); }
		public TextVar 	getTextVar()		{ return textvar; }
		public void 	setText(String val)	{ if (type == NO_VAR) text.setText(val); else textvar.setText(val);}
		public String 	getText()			{ return (type == NO_VAR) ? text.getText() : textvar.getText();	}
		
		@SuppressWarnings("unused")
		public void setVisible(boolean b) {
			if (button != null)  button.setVisible(b);
			if (label != null)   label.setVisible(b);
			if (textvar != null) textvar.setVisible(b);
		}
		
	}
	
	
	public TeraDataBulkLoaderDialog(Shell parent, Object in, TransMeta transMeta, String sname)
	{
		super(parent, (BaseStepMeta)in, transMeta, sname);
		input = (TeraDataBulkLoaderMeta) in;
		inputFields =new HashMap<String, Integer>();
		inputFieldType =new HashMap<String, Integer>();
		inputFieldLength =new HashMap<String, Integer>();
	}

	
	ModifyListener lsTableMod = new ModifyListener() {
		public void modifyText(ModifyEvent arg0) {
			input.setChanged();
			setTableFieldCombo();
		}
	};
	ModifyListener lsMod = new ModifyListener()
	{
		public void modifyText(ModifyEvent e)
		{
			input.setChanged();
		}
	};
	Listener lsModSelect = new Listener()
	{
		public void handleEvent(Event e)
		{
			input.setChanged();
		}
	};
	
	public String open()
	{
		Shell parent = getParent();
		Display display = parent.getDisplay();

		shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
		props.setLook(shell);
		setShellImage(shell, input);

		changed = input.hasChanged();

		FormLayout formLayout = new FormLayout();
		formLayout.marginWidth = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;

		shell.setLayout(formLayout);
		shell.setText(BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.Shell.Title")); 

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
    wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wStepname.setText(stepname);
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( 0, margin );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );
		
    // Connection line
    wConnection = addConnectionLine( shell, wStepname, middle, margin );
    if ( input.getDatabaseMeta() == null && transMeta.nrDatabases() == 1 ) {
      wConnection.select( 0 );
    }
    wConnection.addModifyListener( lsMod );

    //  Generate or Use Script File
    CompositeMenuItem wScriptOption = new CompositeMenuItem(shell, wConnection, "TeraDataBulkLoaderDialog.ScriptOption.Label", 0);
    cScriptOption = wScriptOption.addRadioComposite(TeraDataBulkLoader.ScriptTypes, SWT.NO_RADIO_GROUP, this, new Runnable() {
      
      @Override
      public void run() {
        disableInputs();
      }
    });
// Composite groups and folder
// Shell contains cCommonItems and fItemSet folder.
// fItemSet folder contains Script and Execution tabs, and the composites cExecutionScriptItems and cExecutionItems

		fItemSet = new CTabFolder(shell, SWT.BORDER);
		props.setLook( fItemSet, Props.WIDGET_STYLE_TAB );

		tiExecutionItems     = new CTabItem(fItemSet, SWT.NONE);

		cExecutionItems = new Composite(fItemSet, SWT.BORDER);
		cUseScriptItems = new Composite(fItemSet, SWT.BORDER);
		cGenScriptControlItems = new Composite(fItemSet, SWT.BORDER);
		cGenScriptFields = new Composite(fItemSet, SWT.BORDER);
		cExecutionItems.setLayout (new FormLayout ());
		cUseScriptItems.setLayout (new FormLayout ());
		cGenScriptControlItems.setLayout (new FormLayout ());
		cGenScriptFields.setLayout (new FormLayout ());
		
		props.setLook(cGenScriptControlItems);
		props.setLook(cGenScriptFields);
		props.setLook(cUseScriptItems);
		props.setLook(cExecutionItems);

		tiExecutionItems.setControl(cExecutionItems);
		/************************************************************/
	    tiGenScriptControlItems = new CTabItem(fItemSet, SWT.NONE);
	    tiGenScriptFields       = new CTabItem(fItemSet, SWT.NONE);
	    tiGenScriptControlItems.setText(BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.GenerateScriptControlTab.Label"));
	    tiGenScriptFields.setText(BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.GenerateScriptFieldsTab.Label"));
	    tiGenScriptControlItems.setControl(cGenScriptControlItems);
	    tiGenScriptFields.setControl(cGenScriptFields);
		tiUseScriptItems        = new CTabItem(fItemSet, SWT.NONE);
		tiUseScriptItems.setText(BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.ScriptTab.Label"));
		tiUseScriptItems.setControl(cUseScriptItems);
		/************************************************************/

		tiExecutionItems.setText(BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.ExecutionTab.Label"));
		fItemSet.setSelection(tiExecutionItems);
		
		
		FormData fdExecutionItems       = new FormData();
		fdExecutionItems.left  = new FormAttachment(0, margin);
		fdExecutionItems.top   = new FormAttachment(wScriptOption.getComposite(), margin);
		fdExecutionItems.right = new FormAttachment(100, -margin);
		cExecutionItems.setLayoutData(fdExecutionItems);	

		FormData fdScript       = new FormData();
		fdScript.left  = new FormAttachment(0, margin);
		fdScript.top   = new FormAttachment(cExecutionItems, margin);
		fdScript.right = new FormAttachment(100, -margin);
		fdScript.bottom = new FormAttachment(100, -margin);
		cUseScriptItems.setLayoutData(fdScript);	

		/********************************************************************************************************************/
		/***************  cCommonItems - Common items Composite Group *******************************************************/
		/********************************************************************************************************************/

		
		/********************************************************************************************************************/
		/***************  cExecutionItems --  Execution Environment Composite Group ******************************/
		/********************************************************************************************************************/
		wTdInstall     = new TextVarMenuItem(cExecutionItems,(Control)null, "TeraDataBulkLoaderDialog.TdInstall.Label",DIR_BUTTON);
		wTwbRoot       = new TextVarMenuItem(cExecutionItems,wTdInstall,    "TeraDataBulkLoaderDialog.TwbRoot.Label",DIR_BUTTON);
		wLibPath       = new TextVarMenuItem(cExecutionItems,wTwbRoot,      "TeraDataBulkLoaderDialog.TDLibPath.Label",DIR_BUTTON);
		wTbuildLibPath = new TextVarMenuItem(cExecutionItems,wLibPath,      "TeraDataBulkLoaderDialog.TbuildLibPath.Label",DIR_BUTTON);
		wTdicuLibPath  = new TextVarMenuItem(cExecutionItems,wTbuildLibPath,"TeraDataBulkLoaderDialog.TdicuLibPath.Label",DIR_BUTTON);
		wCopLibPath    = new TextVarMenuItem(cExecutionItems,wTdicuLibPath, "TeraDataBulkLoaderDialog.CopLibPath.Label",DIR_BUTTON);
		wTbuildPath    = new TextVarMenuItem(cExecutionItems,wCopLibPath,   "TeraDataBulkLoaderDialog.TbuildPath.Label",FILE_BUTTON);
		wDataFile      = new TextVarMenuItem(cExecutionItems,wTbuildPath,   "TeraDataBulkLoaderDialog.DataFile.Label",NO_BUTTON);
		wJobName       = new TextVarMenuItem(cExecutionItems,wDataFile,     "TeraDataBulkLoaderDialog.JobName.Label",NO_BUTTON);
	
		/**************************** these in the control tab *****************************************/
		cGenScriptControlItemsDisableMsg = new TextVarMenuItem(cGenScriptControlItems, (Control)null, "TeraDataBulkLoaderDialog.GenScriptControlItemsDisable.Label",LABEL_ONLY | NO_BUTTON);
		wSchema      = new TextVarMenuItem(cGenScriptControlItems, cGenScriptControlItemsDisableMsg, "TeraDataBulkLoaderDialog.TargetSchema.Label",NO_BUTTON);
		wSchema.getTextVar().addModifyListener(lsTableMod);
		wTable       = new TextVarMenuItem(cGenScriptControlItems, wSchema, "TeraDataBulkLoaderDialog.TargetTable.Label",TABLE_BUTTON);
		wTable.getTextVar().addModifyListener(lsTableMod);
		wLogTable    = new TextVarMenuItem(cGenScriptControlItems, wTable, "TeraDataBulkLoaderDialog.LogTable.Label", NO_BUTTON);
		wWorkTable   = new TextVarMenuItem(cGenScriptControlItems, wLogTable, "TeraDataBulkLoaderDialog.WorkTable.Label", NO_BUTTON);
		wErrorTable  = new TextVarMenuItem(cGenScriptControlItems, wWorkTable, "TeraDataBulkLoaderDialog.ErrorTable.Label", NO_BUTTON);
		wErrorTable2 = new TextVarMenuItem(cGenScriptControlItems, wErrorTable, "TeraDataBulkLoaderDialog.ErrorTable2.Label", NO_BUTTON);
	
		CompositeMenuItem wDropTables = new CompositeMenuItem(cGenScriptControlItems,wErrorTable2,"TeraDataBulkLoaderDialog.DropTables.Label",SWT.BORDER);
		wbDropLog    = wDropTables.addButton("TeraDataBulkLoaderDialog.LogTable.Label",    SWT.CHECK);
		wbDropWork   = wDropTables.addButton("TeraDataBulkLoaderDialog.WorkTable.Label",   SWT.CHECK);
		wbDropError  = wDropTables.addButton("TeraDataBulkLoaderDialog.ErrorTable.Label",  SWT.CHECK);
		wbDropError2 = wDropTables.addButton("TeraDataBulkLoaderDialog.ErrorTable2.Label", SWT.CHECK);

		CompositeMenuItem wRowHandling = new CompositeMenuItem(cGenScriptControlItems,wDropTables.getComposite(),"TeraDataBulkLoaderDialog.RowHandling.Label",SWT.BORDER);
		wbIgnoreDupUpdate       = wRowHandling.addButton("TeraDataBulkLoaderDialog.IgnoreDupUpdate.Label",    SWT.CHECK);
		wbInsertMissingUpdate   = wRowHandling.addButton("TeraDataBulkLoaderDialog.InsertMissingUpdate.Label",   SWT.CHECK);
		wbIgnoreMissingUpdate   = wRowHandling.addButton("TeraDataBulkLoaderDialog.IgnoreMissing.Label",  SWT.CHECK);


		wAccessLogFile = new TextVarMenuItem(cGenScriptControlItems, wRowHandling, "TeraDataBulkLoaderDialog.AccessLogFile.Label", FILE_BUTTON);
		wUpdateLogFile = new TextVarMenuItem(cGenScriptControlItems, wAccessLogFile, "TeraDataBulkLoaderDialog.UpdateLogFile.Label", FILE_BUTTON);
		wScriptFile    = new TextVarMenuItem(cGenScriptControlItems, wUpdateLogFile, "TeraDataBulkLoaderDialog.ScriptFile.Label", NO_BUTTON);

		/**************************** these in the fields tab *****************************************/
		cGenScriptFieldsDisableMsg = new TextVarMenuItem(cGenScriptFields, (Control)null, "TeraDataBulkLoaderDialog.GenScriptFieldsDisable.Label",LABEL_ONLY | NO_BUTTON);
		CompositeMenuItem wActionType = new CompositeMenuItem(cGenScriptFields, cGenScriptFieldsDisableMsg,"TeraDataBulkLoaderDialog.ActionType.Label", 0 );
		cActionType = wActionType.addRadioComposite(TeraDataBulkLoader.ActionTypes, SWT.NO_RADIO_GROUP,this, new Runnable() {
      
      @Override
      public void run() {
        disableKeytable();
      }
    });

// Key table
		int nrKeyCols = 3;
		int nrKeyRows = ((input.getKeyStream() != null) && (input.getKeyStream().length > 3) ? input.getKeyStream().length : 3);

		wlKey = new Label(cGenScriptFields, SWT.NONE);
		wlKey.setText(BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.Keys.Label")); 
		props.setLook(wlKey);
		FormData fdlKey = new FormData();
		fdlKey.left = new FormAttachment(0, 0);
		fdlKey.top = new FormAttachment(wActionType.getComposite(), margin);
		wlKey.setLayoutData(fdlKey);
		
		ciKey = new ColumnInfo[nrKeyCols];
		ciKey[0] = new ColumnInfo(BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.ColumnInfo.TableField"),  ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false); 
		ciKey[1] = new ColumnInfo(BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.ColumnInfo.Comparator"), ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "=", "= ~NULL", "<>", "<", "<=",   //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
				">", ">=", "LIKE", "IS NULL", "IS NOT NULL" });   //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
		ciKey[2] = new ColumnInfo(BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.ColumnInfo.StreamField"), ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false); 
		tableFieldColumns.add(ciKey[0]);
		wKey = new TableView(transMeta, cGenScriptFields, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciKey,
				nrKeyRows, lsMod, props);

		wGet = new Button(cGenScriptFields, SWT.PUSH);
		wGet.setText(BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.GetFields.Button")); 
		fdGet = new FormData();
		fdGet.right = new FormAttachment(100, 0);
		fdGet.top = new FormAttachment(wlKey, margin);
		wGet.setLayoutData(fdGet);

		FormData fdKey = new FormData();
		fdKey.left = new FormAttachment(0, 0);
		fdKey.top = new FormAttachment(wlKey, margin);
		fdKey.right = new FormAttachment(wGet, -margin);
		//fdKey.bottom = new FormAttachment(wlKey, 190);
		wKey.setLayoutData(fdKey);

// The field Table
		wlReturn = new Label(cGenScriptFields, SWT.NONE);
		wlReturn.setText(BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.Fields.Label")); 
		props.setLook(wlReturn);
		fdlReturn = new FormData();
		fdlReturn.left = new FormAttachment(0, 0);
		fdlReturn.top = new FormAttachment(wKey, margin);
		wlReturn.setLayoutData(fdlReturn);

		int UpInsCols = 3;
		int UpInsRows = (input.getFieldTable() != null ? input.getFieldTable().length : 1);

		ciReturn    = new ColumnInfo[UpInsCols];
		ciReturn[0] = new ColumnInfo(BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.ColumnInfo.TableField"), ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false); 
		ciReturn[1] = new ColumnInfo(BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.ColumnInfo.StreamField"), ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false); 
		ciReturn[2] = new ColumnInfo(BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.ColumnInfo.UpdateField"), ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {"Y","N"}); 

		tableFieldColumns.add(ciReturn[0]);
		wReturn = new TableView(transMeta, cGenScriptFields, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
				ciReturn, UpInsRows, lsMod, props);

		
		wGetMapping = new Button(cGenScriptFields, SWT.PUSH);
		wGetMapping.setText(BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.GetFields.Label")); 
		fdGetLU = new FormData();
		fdGetLU.top   = new FormAttachment(wlReturn, margin);
		fdGetLU.right = new FormAttachment(100, 0);
		wGetMapping.setLayoutData(fdGetLU);

		wDoMapping = new Button(cGenScriptFields, SWT.PUSH);
		wDoMapping.setText(BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.EditMapping.Label")); 
		fdDoMapping = new FormData();
		fdDoMapping.top   = new FormAttachment(wGetMapping, margin);
		fdDoMapping.right = new FormAttachment(100, 0);
		wDoMapping.setLayoutData(fdDoMapping);
		wDoMapping.addListener(SWT.Selection, new Listener() { 	public void handleEvent(Event arg0) { generateMappings();}});

		fdReturn = new FormData();
		fdReturn.left   = new FormAttachment(0, 0);
		fdReturn.top    = new FormAttachment(wlReturn, margin);
		fdReturn.right  = new FormAttachment(wGetMapping, -margin);
		fdReturn.bottom  = new FormAttachment(100, 0);
		wReturn.setLayoutData(fdReturn);
/********************************************************************************************************************/
/********************************************************* Use Script Options Group ***********************************/
/********************************************************************************************************************/
		
		cUseScriptItemsDisableMsg = new TextVarMenuItem(cUseScriptItems, (Control)null, "TeraDataBulkLoaderDialog.ControlFileDisable.Label",LABEL_ONLY | NO_BUTTON);
		wControlFile = new TextVarMenuItem(cUseScriptItems, cUseScriptItemsDisableMsg, "TeraDataBulkLoaderDialog.ControlFile.Label",FILE_BUTTON);

		CompositeMenuItem wSubstituteControlFile = new CompositeMenuItem(cUseScriptItems, wControlFile, "TeraDataBulkLoaderDialog.SubstituteControlFile.Label",0);
		wbSubstituteControlFile = wSubstituteControlFile.addButton("", SWT.CHECK);

		wVariableFile = new TextVarMenuItem(cUseScriptItems, wSubstituteControlFile, "TeraDataBulkLoaderDialog.VariableFile.Label",FILE_BUTTON);
		
/***************************************************/		

		// THE BUTTONS
		wOK = new Button(shell, SWT.PUSH);
		wOK.setText(BaseMessages.getString(PKG, "System.Button.OK")); 
		wCancel = new Button(shell, SWT.PUSH);
		wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel")); 
		// Preview Button
        wPreviewScript = new Button(shell, SWT.PUSH);
        wPreviewScript.setText("Preview Script");

		setButtonPositions(new Button[] { wOK, wPreviewScript, wCancel }, margin, null);

    FormData fdItemSet       = new FormData();
    fdItemSet.left  = new FormAttachment(0, 0);
    fdItemSet.top   = new FormAttachment(wScriptOption.getComposite(), margin);
    fdItemSet.right = new FormAttachment(100, 0);
    fdItemSet.bottom = new FormAttachment(wOK, -margin);
    fItemSet.setLayoutData(fdItemSet);      
    props.setLook(fItemSet);  
		
		// 
        // Search the fields in the background
        //

        final Runnable runnable = new Runnable()
        {
            public void run()
            {
                StepMeta stepMeta = transMeta.findStep(stepname);
                if (stepMeta!=null)
                {
                    try
                    {
                    	RowMetaInterface row = transMeta.getPrevStepFields(stepMeta);

                        // Remember these fields...
                        for (int i=0;i<row.size();i++)
                        {
                        	inputFields.put(row.getValueMeta(i).getName(), Integer.valueOf(i));
                        	inputFieldType.put(row.getValueMeta(i).getName(), row.getValueMeta(i).getType());
                        	inputFieldLength.put(row.getValueMeta(i).getName(), row.getValueMeta(i).getLength());
                        }

                        setComboBoxes();   
                    }
                    catch(KettleException e)
                    {
                        logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
                    }
                }
            }
        };
        new Thread(runnable).start();
		
		

        // Add listeners
        lsGet = new Listener()
        {
        	public void handleEvent(Event e)
        	{
        		get();
        	}
        };


		lsOK = new Listener()
		{
			public void handleEvent(Event e)
			{
				ok();
			}
		};
		lsGetLU = new Listener()
		{
			public void handleEvent(Event e)
			{
				getUpdate();
			}
		};
//		lsSQL = new Listener()
//		{
//			public void handleEvent(Event e)
//			{
//				create();
//			}
//		};
		 
		lsCancel = new Listener()
		{
			public void handleEvent(Event e)
			{
				cancel();
			}
		};

		lsPreview = new Listener()
		{
			public void handleEvent(Event e)
			{
				previewScript();
			}
		};
		
		wOK.addListener(SWT.Selection, lsOK);
		wGetMapping.addListener(SWT.Selection, lsGetLU);
		wGet.addListener(SWT.Selection, lsGet);
		wCancel.addListener(SWT.Selection, lsCancel);
		wPreviewScript.addListener(SWT.Selection, lsPreview);
		lsDef = new SelectionAdapter() { public void widgetDefaultSelected(SelectionEvent e) { ok(); } };

		wStepname.addSelectionListener(lsDef);
    

		// Detect X or ALT-F4 or something that kills this window...
		shell.addShellListener(new ShellAdapter()
		{
			public void shellClosed(ShellEvent e)
			{
				cancel();
			}
		});

		// Set the shell size, based upon previous time...
		setSize();

		getData();
		setTableFieldCombo();
		input.setChanged(changed);

		disableInputs();
		disableKeytable();
		shell.open();

		while (!shell.isDisposed())
		{
			if (!display.readAndDispatch())
				display.sleep();
		}
		return stepname;
	}
	/**
	 * Reads in the fields from the previous steps and from the ONE next step and opens an 
	 * EnterMappingDialog with this information. After the user did the mapping, those information 
	 * is put into the Select/Rename table.
	 */
	private void generateMappings() {

		// Determine the source and target fields...
		//
		RowMetaInterface sourceFields;
		RowMetaInterface targetFields;

		try {
			sourceFields = transMeta.getPrevStepFields(stepMeta);
		} catch(KettleException e) {
			new ErrorDialog(shell, BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.DoMapping.UnableToFindSourceFields.Title"), BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.DoMapping.UnableToFindSourceFields.Message"), e);
			return;
		}
		// refresh data
		input.setDatabaseMeta(transMeta.findDatabase(wConnection.getText()) );
		input.setTableName(transMeta.environmentSubstitute(wTable.getText()));
		input.setSchemaName(transMeta.environmentSubstitute(wSchema.getText()) );
		StepMetaInterface stepMetaInterface = stepMeta.getStepMetaInterface();
		try {
			targetFields = stepMetaInterface.getRequiredFields(transMeta);
		} catch (KettleException e) {
			new ErrorDialog(shell, BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.DoMapping.UnableToFindTargetFields.Title"), BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.DoMapping.UnableToFindTargetFields.Message"), e);
			return;
		}

		String[] inputNames = new String[sourceFields.size()];
		for (int i = 0; i < sourceFields.size(); i++) {
			ValueMetaInterface value = sourceFields.getValueMeta(i);
			inputNames[i] = value.getName()+
					EnterMappingDialog.STRING_ORIGIN_SEPARATOR+value.getOrigin()+")";
		}

		// Create the existing mapping list...
		//
		List<SourceToTargetMapping> mappings = new ArrayList<SourceToTargetMapping>();
		StringBuffer missingSourceFields = new StringBuffer();
		StringBuffer missingTargetFields = new StringBuffer();

		int nrFields = wReturn.nrNonEmpty();
		for (int i = 0; i < nrFields ; i++) {
			TableItem item = wReturn.getNonEmpty(i);
			String source = item.getText(2);
			String target = item.getText(1);

			int sourceIndex = sourceFields.indexOfValue(source); 
			if (sourceIndex<0) {
				missingSourceFields.append(Const.CR + "   " + source+" --> " + target);
			}
			int targetIndex = targetFields.indexOfValue(target);
			if (targetIndex<0) {
				missingTargetFields.append(Const.CR + "   " + source+" --> " + target);
			}
			if (sourceIndex<0 || targetIndex<0) {
				continue;
			}

			SourceToTargetMapping mapping = new SourceToTargetMapping(sourceIndex, targetIndex);
			mappings.add(mapping);
		}

		// show a confirm dialog if some missing field was found
		//
		if (missingSourceFields.length()>0 || missingTargetFields.length()>0){

			String message="";
			if (missingSourceFields.length()>0) {
				message+=BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.DoMapping.SomeSourceFieldsNotFound", missingSourceFields.toString())+Const.CR;
			}
			if (missingTargetFields.length()>0) {
				message+=BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.DoMapping.SomeTargetFieldsNotFound", missingSourceFields.toString())+Const.CR;
			}
			message+=Const.CR;
			message+=BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.DoMapping.SomeFieldsNotFoundContinue")+Const.CR;
			MessageDialog.setDefaultImage(GUIResource.getInstance().getImageSpoon());
			boolean goOn = MessageDialog.openConfirm(shell, BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.DoMapping.SomeFieldsNotFoundTitle"), message);
			if (!goOn) {
				return;
			}
		}
		EnterMappingDialog d = new EnterMappingDialog(TeraDataBulkLoaderDialog.this.shell, sourceFields.getFieldNames(), targetFields.getFieldNames(), mappings);
		mappings = d.open();

		// mappings == null if the user pressed cancel
		//
		if (mappings!=null) {
			// Clear and re-populate!
			//
			wReturn.table.removeAll();
			wReturn.table.setItemCount(mappings.size());
			for (int i = 0; i < mappings.size(); i++) {
				SourceToTargetMapping mapping = (SourceToTargetMapping) mappings.get(i);
				TableItem item = wReturn.table.getItem(i);
				item.setText(2, sourceFields.getValueMeta(mapping.getSourcePosition()).getName());
				item.setText(1, targetFields.getValueMeta(mapping.getTargetPosition()).getName());
			}
			wReturn.setRowNums();
			wReturn.optWidth(true);
		}
	}

	
	
	public String[] getStreamAndTableFields(){
		List<String> list = new ArrayList<String>();
		
		if (input.getFieldTable() != null) {
			for (int i = 0; i < input.getFieldTable().length; i++) {
				if ((input.getFieldTable()[i] != null) && (input.getFieldStream()[i] != null)){
					list.add(input.getFieldTable()[i] + " = :" + input.getFieldStream()[i]);
				}
			}
		}
		
		String[] simpleArray = new String[ list.size() ];
		return list.toArray( simpleArray );
	}
	
	
	
	/**
	 * Copy information from the meta-data input to the dialog fields.
	 */
	public void getData() {
		if (log.isDebug())
			logDebug(BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.Log.GettingKeyInfo")); 


		if (input.getKeyStream() != null){
			for (int i = 0; i < input.getKeyStream().length; i++) {
				TableItem item = wKey.table.getItem(i);
				if (input.getKeyLookup()[i] != null)
					item.setText(1, input.getKeyLookup()[i]);
				if (input.getKeyCondition()[i] != null)
					item.setText(2, input.getKeyCondition()[i]);
				if (input.getKeyStream()[i] != null)
					item.setText(3, input.getKeyStream()[i]);
			}
		}

		if (input.getFieldTable() != null) {
			logDebug("field table has length "+ input.getFieldTable().length);
			for (int i = 0; i < input.getFieldTable().length; i++) {
				TableItem item = wReturn.table.getItem(i);
				if (input.getFieldTable()[i] != null)
					item.setText(1, input.getFieldTable()[i]);
				if (input.getFieldStream()[i] != null)
					item.setText(2, input.getFieldStream()[i]);
				if (input.getFieldUpdate()[i] == null || input.getFieldUpdate()[i].booleanValue()) {
					item.setText(3, "Y");
				} else {
					item.setText(3, "N");
				}
				logDebug(item.toString());
			}
		}

		// store data from widgets
		String  val;
		int     ival;
		Boolean bval;
		
		/************************** Common variables **************************************/
		if ((val = input.getTbuildPath())     		!= null)		wTbuildPath.setText(val);
		if ((val = input.getTbuildLibPath())     	!= null)		wTbuildLibPath.setText(val);
		if ((val = input.getLibPath())     			!= null)		wLibPath.setText(val);
		if ((val = input.getCopLibPath())     		!= null)		wCopLibPath.setText(val);
		if ((val = input.getTdicuLibPath())     	!= null)		wTdicuLibPath.setText(val);
		if ((val = input.getTdInstallPath())     	!= null)		wTdInstall.setText(val);
		if ((val = input.getTwbRoot())     			!= null)		wTwbRoot.setText(val);
		
		if ((val = input.getJobName()) 				!= null)		wJobName.setText(val);

		if ((bval = input.getGenerateScript())  	!= null)		cScriptOption.setSelection(bval ? 0 : 1);

		/************************** pre-created script option variables ************************/
		if ((val = input.getVariableFile())           != null)	wVariableFile.setText(val);
		if ((val = input.getExistingScriptFile())     != null)	wControlFile.setText(val);
		if ((bval = input.getSubstituteControlFile()) != null)	wbSubstituteControlFile.setSelection(bval);

		/************************** generated option variables     ************************/
		if ((val  = input.getSchemaName())    != null)			wSchema.setText(val);
		if ((val  = input.getTableName())     != null)			wTable.setText(val);
		if ((val  = input.getLogTable())      != null)			wLogTable.setText(val);
		if ((val  = input.getWorkTable())     != null)			wWorkTable.setText(val);

		if ((val  = input.getErrorTable())    != null)			wErrorTable.setText(val);
		if ((val  = input.getErrorTable2())   != null)			wErrorTable2.setText(val);
		if ((bval = input.getDropLogTable())      != null)		wbDropLog.setSelection(bval);
		if ((bval = input.getDropWorkTable())     != null)		wbDropWork.setSelection(bval);
		if ((bval = input.getDropErrorTable())    != null)		wbDropError.setSelection(bval);
		if ((bval = input.getDropErrorTable2())   != null)		wbDropError2.setSelection(bval);
		if ((bval = input.getIgnoreDupUpdate())      != null)		wbIgnoreDupUpdate.setSelection(bval);
		if ((bval = input.getInsertMissingUpdate())      != null)		wbInsertMissingUpdate.setSelection(bval);
		if ((bval = input.getIgnoreMissingUpdate())      != null)		wbIgnoreMissingUpdate.setSelection(bval);
		if ((val  = input.getAccessLogFile()) != null)			wAccessLogFile.setText(val);
		if ((val  = input.getUpdateLogFile()) != null)			wUpdateLogFile.setText(val);
		if ((val  = input.getFifoFileName())   != null)			wDataFile.setText(val);
		if ((val  = input.getScriptFileName()) != null)			wScriptFile.setText(val);
		
		if ((ival = input.getActionType())     >= 0)            cActionType.setSelection(ival);

		if (input.getDatabaseMeta()  != null)			
			wConnection.setText(input.getDatabaseMeta().getName());
		else {
			if (transMeta.nrDatabases() == 1) {
				wConnection.setText(transMeta.getDatabase(0).getName());
			}
		}
		wReturn.setRowNums();
		wReturn.optWidth(true);

		wStepname.selectAll();
		wStepname.setFocus();
	}

	private void setTableFieldCombo(){
		Runnable fieldLoader = new Runnable() {
			public void run() { 
			   if (!wTable.getTextVar().isDisposed() &&!wConnection.isDisposed()) {
   				//clear
   				for (int i = 0; i < tableFieldColumns.size(); i++) {
   					ColumnInfo colInfo = tableFieldColumns.get(i);
   					colInfo.setComboValues(new String[] {});
   				}
   				if (!Const.isEmpty(wTable.getText())) {
   					DatabaseMeta ci = transMeta.findDatabase(wConnection.getText());
   					if (ci != null) {
   						Database db = new Database(loggingObject, ci);
   						try {
   							db.connect();
   
   							String schemaTable = ci	.getQuotedSchemaTableCombination(transMeta.environmentSubstitute(wSchema
   											.getText()), transMeta.environmentSubstitute(wTable.getText()));
   							RowMetaInterface r = db.getTableFields(schemaTable);
   							if (null != r) {
   								String[] fieldNames = r.getFieldNames();
   								if (null != fieldNames) {
   									for (int i = 0; i < tableFieldColumns.size(); i++) {
   										ColumnInfo colInfo = tableFieldColumns.get(i);
   										colInfo.setComboValues(fieldNames);
   									}
   								}
   							}
   						} catch (Exception e) {
   							for (int i = 0; i < tableFieldColumns.size(); i++) {
   								ColumnInfo colInfo = tableFieldColumns	.get(i);
   								colInfo.setComboValues(new String[] {});
   							}
   							// ignore any errors here. drop downs will not be
   							// filled, but no problem for the user
   						}
   					}
   				}
   			}
			}
		};
		shell.getDisplay().asyncExec(fieldLoader);
	}
	
	protected void setComboBoxes()
	{
		// Something was changed in the row.
		//
		final Map<String, Integer> fields = new HashMap<String, Integer>();

		// Add the currentMeta fields...
		fields.putAll(inputFields);

		Set<String> keySet = fields.keySet();
		List<String> entries = new ArrayList<String>(keySet);

		String[] fieldNames= (String[]) entries.toArray(new String[entries.size()]);
		Const.sortStrings(fieldNames);
		// return fields
		ciReturn[1].setComboValues(fieldNames);
	    ciKey[2].setComboValues(fieldNames);
	    //ciKey[3].setComboValues(fieldNames);
	}
	private void cancel()
	{
		stepname = null;
		input.setChanged(changed);
		dispose();
	}

	private void getInfo(TeraDataBulkLoaderMeta inf)
	{
		int nrkeys = wKey.nrNonEmpty();
		int nrfields = wReturn.nrNonEmpty();

		inf.allocate(nrkeys, nrfields);

		//inf.setLocalFile( wLocal.getSelection() );

		if(log.isDebug()) logDebug(BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.Log.FoundFields", "" + nrfields));  
		for (int i = 0; i < nrkeys; i++) {
			TableItem item = wKey.getNonEmpty(i);
			inf.getKeyLookup()[i] = item.getText(1);
			inf.getKeyCondition()[i] = item.getText(2);
			inf.getKeyStream()[i] = item.getText(3);
		}

		for (int i = 0; i < nrfields; i++) {
			TableItem item = wReturn.getNonEmpty(i);
			inf.getFieldTable()[i] = item.getText(1);
			inf.getFieldStream()[i] = item.getText(2);
			inf.getFieldUpdate()[i] = Boolean.valueOf("Y".equals(item.getText(3)));
		}

		// populate meta from widgets
		inf.setTbuildPath(wTbuildPath.getText());
		inf.setTbuildLibPath(wTbuildLibPath.getText());
		inf.setLibPath(wLibPath.getText());
		inf.setCopLibPath(wCopLibPath.getText());
		inf.setTdicuLibPath(wTdicuLibPath.getText());
		inf.setTdInstallPath(wTdInstall.getText());
		inf.setTwbRoot(wTwbRoot.getText());
		
		inf.setJobName(wJobName.getText());

		inf.setGenerateScript(cScriptOption.getSelection() == 0);
		
		// Use script options
		inf.setExistingScriptFile(wControlFile.getText());                     
		inf.setSubstituteControlFile(wbSubstituteControlFile.getSelection());
		inf.setVariableFile(wVariableFile.getText());

		// Generate script options
		inf.setSchemaName( wSchema.getText() );
		inf.setTableName( wTable.getText() );
		inf.setLogTable( wLogTable.getText() );
		inf.setWorkTable( wWorkTable.getText() );

		inf.setErrorTable( wErrorTable.getText() );
		inf.setErrorTable2( wErrorTable2.getText() );
		inf.setDropLogTable(wbDropLog.getSelection());
		inf.setDropWorkTable(wbDropWork.getSelection());
		
		inf.setDropErrorTable(wbDropError.getSelection());
		inf.setDropErrorTable2(wbDropError2.getSelection());
		inf.setIgnoreDupUpdate(wbIgnoreDupUpdate.getSelection());
		inf.setInsertMissingUpdate(wbInsertMissingUpdate.getSelection());
		inf.setIgnoreMissingUpdate(wbIgnoreMissingUpdate.getSelection());
		inf.setAccessLogFile( wAccessLogFile.getText() );
		inf.setUpdateLogFile( wUpdateLogFile.getText() );
		inf.setFifoFileName(wDataFile.getText());
		inf.setScriptFileName(wScriptFile.getText());
		inf.setActionType(cActionType.getSelection());
		inf.setDatabaseMeta(  transMeta.findDatabase(wConnection.getText()) );

		stepname = wStepname.getText(); // return value
	}

	private class requiredFieldsError {
		private StringBuffer messages;
		private String       title;
		private int          msgcount = 0;
		
		requiredFieldsError(String s, String msg) {
			title = s;
			messages = new StringBuffer(msg + "\n");
		}
		
		public void addMessage(String s){
			messages.append(s + "\n");
			msgcount++;
		}
		
		public boolean hasErrors(){
			return msgcount > 0;
		}
		
		public void display() {
			MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
			mb.setMessage(messages.toString()); 
			mb.setText(title); 
			mb.open();
		}

		public void addIfUndef(TextVarMenuItem text, String msg) {
			if (Const.isEmpty(text.getText())){
				addMessage(BaseMessages.getString(PKG, msg));
			}
		}
		
		public void addIfUndef(Text text, String msg) {
      if (Const.isEmpty(text.getText())){
        addMessage(BaseMessages.getString(PKG, msg));
      }
    }

		public void addIfUndef(CCombo text, String msg) {
			if (Const.isEmpty(text.getText())){
				addMessage(BaseMessages.getString(PKG, msg));
			}
		}

		@SuppressWarnings("unused")
		public void addIfUndef(DatabaseMeta databaseMeta, String msg) {
			if (databaseMeta == null){
				addMessage(BaseMessages.getString(PKG, msg));
			}
		}
	}

	private void ok() {
		final requiredFieldsError  errorPopup = new requiredFieldsError(
				BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.MissingRequiredTitle.DialogMessage"),
				BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.MissingRequiredMsg.DialogMessage")
				);
		
		// Always required
		errorPopup.addIfUndef(wStepname,           "TeraDataBulkLoaderDialog.MissingStepname.DialogMessage");
		errorPopup.addIfUndef(wConnection,            "TeraDataBulkLoaderDialog.InvalidConnection.DialogMessage");  
		errorPopup.addIfUndef(wTdInstall,             "TeraDataBulkLoaderDialog.MissingInstallPath.DialogMessage");

		// Required depending on option
		if (cScriptOption.getSelection() == 0){
			errorPopup.addIfUndef(wLogTable,          "TeraDataBulkLoaderDialog.MissingLogTable.DialogMessage");
			errorPopup.addIfUndef(wTable,             "TeraDataBulkLoaderDialog.MissingTargetTable.DialogMessage");
			errorPopup.addIfUndef(wSchema,            "TeraDataBulkLoaderDialog.MissingSchema.DialogMessage");
		}else{
			errorPopup.addIfUndef(wControlFile,       "TeraDataBulkLoaderDialog.MissingControlFile.DialogMessage");
		}

		// check for errors? 
		if (errorPopup.hasErrors()){
			errorPopup.display();
		}else{
			getInfo(input);
			dispose();
		}
	}

	
	/**
	 * Disable inputs.
	 */
	public void disableInputs() {
		int choice = cScriptOption.getSelection();
		if (choice == 0){
			// enable disable as appropriate
			wPreviewScript.setEnabled(true);
			cGenScriptControlItems.setEnabled(true);
			cGenScriptFields.setEnabled(true);
		    cUseScriptItems.setEnabled(false);
		    // change visibility of disable messages
		    cGenScriptControlItemsDisableMsg.setVisible(false);
		    cGenScriptFieldsDisableMsg.setVisible(false);
		    cUseScriptItemsDisableMsg.setVisible(true);

		    // select appropriate tab
		    fItemSet.setSelection(tiGenScriptControlItems);
		}else{
			// enable disable as appropriate
			wPreviewScript.setEnabled(false);
			cGenScriptControlItems.setEnabled(false);
			cGenScriptFields.setEnabled(false);
		    cUseScriptItems.setEnabled(true); 
		    // change visibility of disable messages
		    cUseScriptItemsDisableMsg.setVisible(false);
		    cGenScriptControlItemsDisableMsg.setVisible(true);
		    cGenScriptFieldsDisableMsg.setVisible(true);
		    // select appropriate tab
		    fItemSet.setSelection(tiUseScriptItems);
		}
	}

	public void disableKeytable() {
		int choice = cActionType.getSelection();
		System.out.println("Choice is "+choice);
		if (choice == 0){
			wKey.setEnabled(false);
			wKey.setVisible(false);
			wGet.setEnabled(false);
			wGet.setVisible(false);
			wlKey.setVisible(false);
		}else{
			wKey.setEnabled(true);
			wKey.setVisible(true);
			wGet.setEnabled(true);
			wGet.setVisible(true);
			wlKey.setVisible(true);
		}
	}
	
	
	private void get()
	{
		try
		{
			RowMetaInterface r = transMeta.getPrevStepFields(stepname);
			if (r != null)
			{
                TableItemInsertListener listener = new TableItemInsertListener()
                {
                    public boolean tableItemInserted(TableItem tableItem, ValueMetaInterface v)
                    {
                        tableItem.setText(2, "=");
                        return true;
                    }
                };
                BaseStepDialog.getFieldsFromPrevious(r, wKey, 1, new int[] { 1, 3}, new int[] {}, -1, -1, listener);
			}
		}
		catch (KettleException ke)
		{
			new ErrorDialog(shell, BaseMessages.getString(PKG, "InsertUpdateDialog.FailedToGetFields.DialogTitle"), 
					BaseMessages.getString(PKG, "InsertUpdateDialog.FailedToGetFields.DialogMessage"), ke); 
		}
	}
	
	
	private void getUpdate()
	{
		try
		{
			RowMetaInterface r = transMeta.getPrevStepFields(stepname);
			if (r != null)
			{
				TableItemInsertListener listener = new TableItemInsertListener()
				{
					public boolean tableItemInserted(TableItem tableItem, ValueMetaInterface v)
					{
						if ( v.getType() == ValueMetaInterface.TYPE_DATE )
						{
							// The default is : format is OK for dates, see if this sticks later on...
							//
							tableItem.setText(3, "Y");
						}
						else
						{
							tableItem.setText(3, "Y"); // default is OK too...
						}
						return true;
					}
				};
				BaseStepDialog.getFieldsFromPrevious(r, wReturn, 1, new int[] { 1, 2}, new int[] {}, -1, -1, listener);
			}
		}
		catch (KettleException ke)
		{
			new ErrorDialog(shell, BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.FailedToGetFields.DialogTitle"), 
					BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.FailedToGetFields.DialogMessage"), ke); 
		}
	}

	// Generate code for create table...
	// Conversions done by Database
//	private void create()
//	{
//		try
//		{
//			TeraDataBulkLoaderMeta info = new TeraDataBulkLoaderMeta();
//			getInfo(info);
//
//			String name = stepname; // new name might not yet be linked to other steps!
//			StepMeta stepMeta = new StepMeta(BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.StepMeta.Title"), name, info); 
//			RowMetaInterface prev = transMeta.getPrevStepFields(stepname);
//
//			SQLStatement sql = info.getSQLStatements(transMeta, stepMeta, prev, repository);
//			if (!sql.hasError())
//			{
//				if (sql.hasSQL())
//				{
//					SQLEditor sqledit = new SQLEditor(transMeta, shell, SWT.NONE, info.getDatabaseMeta(), transMeta.getDbCache(),
//							sql.getSQL());
//					sqledit.open();
//				}
//				else
//				{
//					MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
//					mb.setMessage(BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.NoSQLNeeds.DialogMessage")); 
//					mb.setText(BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.NoSQLNeeds.DialogTitle")); 
//					mb.open();
//				}
//			}
//			else
//			{
//				MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
//				mb.setMessage(sql.getError());
//				mb.setText(BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.SQLError.DialogTitle")); 
//				mb.open();
//			}
//		}
//		catch (KettleException ke)
//		{
//			new ErrorDialog(shell, BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.CouldNotBuildSQL.DialogTitle"), 
//					BaseMessages.getString(PKG, "TeraDataBulkLoaderDialog.CouldNotBuildSQL.DialogMessage"), ke); 
//		}
//
//	}

	public void previewScript(){
		TeraDataBulkLoaderMeta metacopy = new TeraDataBulkLoaderMeta();
		getInfo(metacopy);
		TeraDataBulkLoaderRoutines routines = new TeraDataBulkLoaderRoutines(null,metacopy);
		try {
			String script = routines.createGeneratedScriptFile(inputFieldType,inputFieldLength);
			Shell mb = new Shell();
			mb.setLayout(new FillLayout());
			Text msg = new Text(mb, SWT.BORDER_SOLID | SWT.MULTI | SWT.V_SCROLL);
			mb.setText("Preview");
			msg.setText(script); 
			msg.setEditable(false);
			//mb.add(msg);
			mb.open();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
