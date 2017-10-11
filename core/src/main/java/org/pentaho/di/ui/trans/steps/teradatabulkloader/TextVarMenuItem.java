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
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.widget.TextVar;

/**
 * The Class TextVarMenuItem.
 */
public abstract class TextVarMenuItem {
  /** The label. */
  private Label label;

  /** The button. */
  private Button button;

  /** The textvar. */
  private TextVar textvar;

  /** The text. */
  private Text text;

  /** The type. */
  private int type;

  /** The margin. */
  int margin = Const.MARGIN;

  /** The middle. */
  final int middle;

  SelectionListener selectionListener = null;

  /**
   * Instantiates a new text var menu item.
   * 
   * @param parent
   *          the parent
   * @param top
   *          the top
   * @param labelProp
   *          the label prop
   * @param buttonType
   *          the button type
   * @param teraDataBulkLoaderDialog
   *          TODO
   */
  TextVarMenuItem( Composite parent, PropsUI props, TransMeta transMeta, ModifyListener lsMod, TextVarMenuItem top,
      String labelProp, int buttonType ) {
    this( parent, props, transMeta, lsMod, top.getButton(), labelProp, buttonType );
  }

  /**
   * Instantiates a new text var menu item.
   * 
   * @param parent
   *          the parent
   * @param top
   *          the top
   * @param labelProp
   *          the label prop
   * @param buttonType
   *          the button type
   * @param teraDataBulkLoaderDialog
   *          TODO
   */
  TextVarMenuItem( Composite parent, PropsUI props, TransMeta transMeta, ModifyListener lsMod, CompositeMenuItem top,
      String labelProp, int buttonType ) {
    this( parent, props, transMeta, lsMod, top.getComposite(), labelProp, buttonType );
  }

  /**
   * Instantiates a new text var menu item.
   * 
   * @param parent
   *          the parent
   * @param top
   *          the top
   * @param labelProp
   *          the label prop
   * @param buttonType
   *          the button type
   * @param teraDataBulkLoaderDialog
   *          TODO
   */
  TextVarMenuItem( Composite parent, PropsUI props, TransMeta transMeta, ModifyListener lsMod, Control top,
      String labelProp, int buttonType ) {
    this.middle = props.getMiddlePct();
    type = buttonType;

    button = new Button( parent, SWT.PUSH | SWT.CENTER );
    button.setText( BaseMessages.getString( TeraDataBulkLoaderDialog.PKG, "TeraDataBulkLoaderDialog.Browse.Button" ) );
    props.setLook( button );
    FormData fdb = new FormData();
    if ( top == null ) {
      fdb.top = new FormAttachment( 0, margin );
    } else {
      fdb.top = new FormAttachment( top, margin );
    }
    fdb.right = new FormAttachment( 100, -margin );
    button.setLayoutData( fdb );
    if ( type == TeraDataBulkLoaderDialog.NO_BUTTON || type == TeraDataBulkLoaderDialog.NO_VAR
        || type == TeraDataBulkLoaderDialog.LABEL_ONLY ) {
      button.setVisible( false );
      button.dispose();
      button = null;
    }

    label = new Label( parent, SWT.RIGHT );
    label.setText( BaseMessages.getString( TeraDataBulkLoaderDialog.PKG, labelProp ) );
    FormData fdl = new FormData();
    fdl.left = new FormAttachment( 0, margin );
    if ( top == null ) {
      fdl.top = new FormAttachment( 0, margin );
    } else {
      fdl.top = new FormAttachment( top, margin );
    }
    if ( ( type & TeraDataBulkLoaderDialog.LABEL_ONLY ) != 0 ) {
      fdl.right = new FormAttachment( 100, -margin );
      label.setLayoutData( fdl );
      label.setAlignment( SWT.CENTER );
      return;
    }
    fdl.right = new FormAttachment( middle, -margin );
    props.setLook( label );
    label.setLayoutData( fdl );

    FormData fdt = new FormData();
    fdt.left = new FormAttachment( middle, margin );
    if ( top == null ) {
      fdt.top = new FormAttachment( 0, margin );
    } else {
      fdt.top = new FormAttachment( top, margin );
    }
    if ( type == TeraDataBulkLoaderDialog.NO_BUTTON || type == TeraDataBulkLoaderDialog.NO_VAR
        || type == TeraDataBulkLoaderDialog.LABEL_ONLY ) {
      fdt.right = new FormAttachment( 100, -margin );
    } else {
      fdt.right = new FormAttachment( button, -margin );
    }
    if ( type == TeraDataBulkLoaderDialog.NO_VAR ) {
      text = new Text( parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
      props.setLook( text );
      text.addModifyListener( lsMod );
      text.setLayoutData( fdt );
    } else {
      textvar = new TextVar( transMeta, parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
      props.setLook( textvar );
      textvar.addModifyListener( lsMod );
      textvar.setLayoutData( fdt );
    }

    if ( textvar != null ) {
      if ( button != null ) {
        selectionListener = new SelectionListener() {
          private SelectionListener selectionListener = null;
          private boolean initialized = false;

          private void initIfNecessary() {
            if ( !initialized ) {
              selectionListener = createSelectionListener( textvar );
              initialized = true;
            }
          }

          @Override
          public void widgetSelected( SelectionEvent arg0 ) {
            initIfNecessary();
            if ( selectionListener != null ) {
              selectionListener.widgetSelected( arg0 );
            }
          }

          @Override
          public void widgetDefaultSelected( SelectionEvent arg0 ) {
            initIfNecessary();
            if ( selectionListener != null ) {
              selectionListener.widgetDefaultSelected( arg0 );
            }
          }
        };
        button.addSelectionListener( selectionListener );
      }
    }
  }

  /**
   * Gets the button.
   * 
   * @return the button
   */
  public Control getButton() {
    return button != null ? button : ( textvar != null ? textvar : label );
  }

  /**
   * Gets the text var.
   * 
   * @return the text var
   */
  public TextVar getTextVar() {
    return textvar;
  }

  /**
   * Sets the text.
   * 
   * @param val
   *          the new text
   */
  public void setText( String val ) {
    if ( type == TeraDataBulkLoaderDialog.NO_VAR ) {
      text.setText( val );
    } else {
      textvar.setText( val );
    }
  }

  /**
   * Gets the text.
   * 
   * @return the text
   */
  public String getText() {
    return ( type == TeraDataBulkLoaderDialog.NO_VAR ) ? text.getText() : textvar.getText();
  }

  /**
   * Sets the visible.
   * 
   * @param b
   *          the new visible
   */
  public void setVisible( boolean b ) {
    if ( button != null ) {
      button.setVisible( b );
    }
    if ( label != null ) {
      label.setVisible( b );
    }
    if ( textvar != null ) {
      textvar.setVisible( b );
    }
  }

  protected abstract SelectionListener createSelectionListener( TextVar textVar );
}
