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
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.ui.core.widget.TextVar;

/**
 * The listener interface for receiving fileSelectionButton events.
 * The class that is interested in processing a fileSelectionButton
 * event implements this interface, and the object created
 * with that class is registered with a component using the
 * component's <code>addFileSelectionButtonListener<code> method. When
 * the fileSelectionButton event occurs, that object's appropriate
 * method is invoked.
 *
 * @see FileSelectionButtonEvent
 */
class FileSelectionButtonListener extends SelectionAdapter {
  /** The text widget. */
  private final TextVar textWidget;
  private final Shell shell;

  /**
   * Instantiates a new file selection button listener.
   *
   * @param textWidget the text widget
   * @param teraDataBulkLoaderDialog TODO
   */
  FileSelectionButtonListener( TextVar textWidget, Shell shell ) {
    this.textWidget = textWidget;
    this.shell = shell;
  }

  /* (non-Javadoc)
   * @see org.eclipse.swt.events.SelectionAdapter#widgetSelected(org.eclipse.swt.events.SelectionEvent)
   */
  @Override
  public void widgetSelected( SelectionEvent e ) {
    FileDialog dialog = new FileDialog( shell, SWT.OPEN );
    if ( dialog.open() != null ) {
      String str = dialog.getFilterPath() + System.getProperty( "file.separator" ) + dialog.getFileName();
      this.textWidget.setText( str );
    }
  }
}
