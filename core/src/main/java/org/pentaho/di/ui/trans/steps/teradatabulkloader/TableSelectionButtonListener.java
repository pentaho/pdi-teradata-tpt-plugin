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

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.database.dialog.DatabaseExplorerDialog;
import org.pentaho.di.ui.core.widget.TextVar;

/**
 * The listener interface for receiving tableSelectionButton events. The class that is interested in processing a
 * tableSelectionButton event implements this interface, and the object created with that class is registered with a
 * component using the component's <code>addTableSelectionButtonListener<code> method. When
 * the tableSelectionButton event occurs, that object's appropriate
 * method is invoked.
 * 
 * @see TableSelectionButtonEvent
 */
class TableSelectionButtonListener extends SelectionAdapter {
  private final TransMeta transMeta;
  private final Shell shell;
  private final TextVarMenuItem wSchema;
  private final TextVarMenuItem wTable;
  private final CCombo wConnection;
  private final Runnable setTableFieldCombo;
  private final LogChannelInterface logChannelInterface;
  /** The text widget. */
  private final TextVar textWidget;

  /**
   * Instantiates a new table selection button listener.
   * 
   * @param textWidget
   *          the text widget
   * @param teraDataBulkLoaderDialog
   *          TODO
   */
  TableSelectionButtonListener( TransMeta transMeta, TextVar textWidget, CCombo wConnection, Shell shell,
      TextVarMenuItem wSchema, TextVarMenuItem wTable, Runnable setTableFieldCombo,
      LogChannelInterface logChannelInterface ) {
    super();
    this.transMeta = transMeta;
    this.textWidget = textWidget;
    this.shell = shell;
    this.wSchema = wSchema;
    this.wTable = wTable;
    this.setTableFieldCombo = setTableFieldCombo;
    this.wConnection = wConnection;
    this.logChannelInterface = logChannelInterface;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.swt.events.SelectionAdapter#widgetSelected(org.eclipse.swt.events.SelectionEvent)
   */
  @Override
  public void widgetSelected( SelectionEvent e ) {
    DatabaseMeta inf = null;
    // New class: SelectTableDialog
    int connr = wConnection.getSelectionIndex();
    if ( connr >= 0 ) {
      inf = transMeta.getDatabase( connr );
    }

    if ( inf != null ) {
      if ( logChannelInterface.isDebug() ) {
        logChannelInterface.logDebug( BaseMessages.getString( TeraDataBulkLoaderDialog.PKG,
            "TeraDataBulkLoaderDialog.Log.LookingAtConnection" )
            + inf.toString() );
      }

      DatabaseExplorerDialog std = new DatabaseExplorerDialog( shell, SWT.NONE, inf, transMeta.getDatabases() );
      std.setSelectedSchemaAndTable( wSchema.getText(), wTable.getText() );
      if ( std.open() ) {
        wSchema.setText( Const.NVL( std.getSchemaName(), "" ) );
        textWidget.setText( Const.NVL( std.getTableName(), "" ) );
        setTableFieldCombo.run();
      }
    } else {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( TeraDataBulkLoaderDialog.PKG,
          "TeraDataBulkLoaderDialog.InvalidConnection.DialogMessage" ) );
      mb.setText( BaseMessages.getString( TeraDataBulkLoaderDialog.PKG,
          "TeraDataBulkLoaderDialog.InvalidConnection.DialogTitle" ) );
      mb.open();
    }
  }
}
