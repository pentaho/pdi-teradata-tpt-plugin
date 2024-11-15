/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.di.ui.trans.steps.teradatabulkloader;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.i18n.BaseMessages;

/**
 * The Class requiredFieldsError.
 */
class RequiredFieldsError {

  /**
   * 
   */
  private final Shell shell;

  /** The messages. */
  private StringBuffer messages;

  /** The title. */
  private String title;

  /** The msgcount. */
  private int msgcount = 0;

  /**
   * Instantiates a new required fields error.
   * 
   * @param shell
   *          the shell
   * @param s
   *          the s
   * @param msg
   *          the msg
   */
  RequiredFieldsError( Shell shell, String s, String msg ) {
    this.shell = shell;
    title = s;
    messages = new StringBuffer( msg + "\n" );
  }

  /**
   * Adds the message.
   * 
   * @param s
   *          the s
   */
  public void addMessage( String s ) {
    messages.append( " - " + s + "\n" );
    msgcount++;
  }

  /**
   * Checks for errors.
   * 
   * @return true, if successful
   */
  public boolean hasErrors() {
    return msgcount > 0;
  }

  /**
   * Display.
   */
  public void display() {
    MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
    mb.setMessage( messages.toString() );
    mb.setText( title );
    mb.open();
  }

  /**
   * Adds the if undef.
   * 
   * @param text
   *          the text
   * @param msg
   *          the msg
   */
  public void addIfUndef( Text text, String msg ) {
    if ( Const.isEmpty( text.getText() ) ) {
      addMessage( BaseMessages.getString( TeraDataBulkLoaderDialog.PKG, msg ) );
    }
  }

  /**
   * Adds the if undef.
   * 
   * @param text
   *          the text
   * @param msg
   *          the msg
   */
  public void addIfUndef( CCombo text, String msg ) {
    if ( Const.isEmpty( text.getText() ) ) {
      addMessage( BaseMessages.getString( TeraDataBulkLoaderDialog.PKG, msg ) );
    }
  }

  /**
   * Adds the if undef.
   * 
   * @param databaseMeta
   *          the database meta
   * @param msg
   *          the msg
   */
  public void addIfUndef( DatabaseMeta databaseMeta, String msg ) {
    if ( databaseMeta == null ) {
      addMessage( BaseMessages.getString( TeraDataBulkLoaderDialog.PKG, msg ) );
    }
  }
}
