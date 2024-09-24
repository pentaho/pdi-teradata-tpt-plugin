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

package org.pentaho.di.ui.trans.steps.teradatabulkloader;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.ui.core.widget.TextVar;

/**
 * The listener interface for receiving directorySelectionButton events. The class that is interested in processing a
 * directorySelectionButton event implements this interface, and the object created with that class is registered with a
 * component using the component's <code>addDirectorySelectionButtonListener<code> method. When
 * the directorySelectionButton event occurs, that object's appropriate
 * method is invoked.
 * 
 * @see DirectorySelectionButtonEvent
 */
class DirectorySelectionButtonListener extends SelectionAdapter {
  private final Shell shell;
  /** The text widget. */
  private final TextVar textWidget;

  /**
   * Instantiates a new directory selection button listener.
   * 
   * @param textWidget
   *          the text widget
   * @param teraDataBulkLoaderDialog
   *          TODO
   */
  DirectorySelectionButtonListener( TextVar textWidget, Shell shell ) {
    this.textWidget = textWidget;
    this.shell = shell;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.swt.events.SelectionAdapter#widgetSelected(org.eclipse.swt.events.SelectionEvent)
   */
  @Override
  public void widgetSelected( SelectionEvent e ) {
    DirectoryDialog dialog = new DirectoryDialog( shell, SWT.OPEN );
    if ( dialog.open() != null ) {
      String str = dialog.getFilterPath() + System.getProperty( "file.separator" ) + dialog.getText();
      this.textWidget.setText( str );
    }
  }
}
