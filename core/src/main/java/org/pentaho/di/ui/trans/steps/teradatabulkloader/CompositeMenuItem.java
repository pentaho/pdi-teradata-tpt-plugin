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
import java.util.List;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.RowLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.ui.core.PropsUI;

/**
 * The Class CompositeMenuItem.
 */
class CompositeMenuItem {

  private final PropsUI props;

  private final ModifyListener lsMod;
  private final Listener lsModSelect;
  private final BaseStepMeta baseStepMeta;

  /** The label. */
  private Label label;

  /** The composite. */
  private Composite composite;

  /** The items. */
  private List<Control> items = new ArrayList<Control>();

  /** The margin. */
  int margin = Const.MARGIN;

  /** The middle. */
  final int middle;

  /**
   * Instantiates a new composite menu item.
   * 
   * @param parent
   *          the parent
   * @param top
   *          the top
   * @param labelProp
   *          the label prop
   * @param type
   *          the type
   * @param teraDataBulkLoaderDialog
   *          TODO
   */
  CompositeMenuItem( PropsUI props, ModifyListener lsMod, Listener lsModSelect, BaseStepMeta baseStepMeta,
      Composite parent, TextVarMenuItem top, String labelProp, int type ) {
    this( props, lsMod, lsModSelect, baseStepMeta, parent, top.getButton(), labelProp, type );
  }

  /**
   * Instantiates a new composite menu item.
   * 
   * @param parent
   *          the parent
   * @param top
   *          the top
   * @param labelProp
   *          the label prop
   * @param type
   *          the type
   * @param teraDataBulkLoaderDialog
   *          TODO
   */
  CompositeMenuItem( PropsUI props, ModifyListener lsMod, Listener lsModSelect, BaseStepMeta baseStepMeta,
      Composite parent, Control top, String labelProp, int type ) {
    this.props = props;
    middle = props.getMiddlePct();
    this.lsMod = lsMod;
    this.lsModSelect = lsModSelect;
    this.baseStepMeta = baseStepMeta;
    label = new Label( parent, SWT.RIGHT );
    label.setText( BaseMessages.getString( TeraDataBulkLoaderDialog.PKG, labelProp ) );
    FormData fdl = new FormData();
    fdl.left = new FormAttachment( 0, margin );
    if ( top == null ) {
      fdl.top = new FormAttachment( 0, margin );
    } else {
      fdl.top = new FormAttachment( top, margin );
    }
    fdl.right = new FormAttachment( middle, -margin );
    props.setLook( label );
    label.setLayoutData( fdl );

    composite = new Composite( parent, SWT.NONE );
    composite.setLayout( new FormLayout() );

    FormData fdc = new FormData();
    fdc.left = new FormAttachment( label, 0 );
    if ( top == null ) {
      fdc.top = new FormAttachment( 0, 0 );
    } else {
      fdc.top = new FormAttachment( top, 0 );
    }
    fdc.right = new FormAttachment( 100, 0 );
    composite.setLayoutData( fdc );
    props.setLook( composite );
  }

  /**
   * Gets the composite.
   * 
   * @return the composite
   */
  public Composite getComposite() {
    return composite;
  }

  /**
   * Adds the.
   * 
   * @param item
   *          the item
   */
  public void add( Control item ) {
    FormData fd = new FormData();
    fd.top = new FormAttachment( 0, margin );
    if ( items.size() > 0 ) {
      fd.left = new FormAttachment( items.get( items.size() - 1 ), margin );
    } else {
      fd.left = new FormAttachment( 0, margin );
    }
    item.setLayoutData( fd );
    props.setLook( item );
    items.add( item );
  }

  /**
   * Adds the c combo.
   * 
   * @return the c combo
   */
  public CCombo addCCombo() {
    CCombo combo = new CCombo( composite, SWT.BORDER | SWT.READ_ONLY );
    combo.setEditable( true );
    props.setLook( combo );
    combo.addModifyListener( lsMod );
    add( combo );
    return combo;
  }

  /**
   * Adds the button.
   * 
   * @param label
   *          the label
   * @param radio
   *          the radio
   * @return the button
   */
  public Button addButton( String label, int radio ) {
    Button button = new Button( composite, radio | SWT.RIGHT );
    if ( !Const.isEmpty( label ) ) {
      button.setText( BaseMessages.getString( TeraDataBulkLoaderDialog.PKG, label ) );
    }
    button.addListener( SWT.CHECK, lsModSelect );
    props.setLook( button );
    add( button );
    return button;
  }

  /**
   * Adds the radio composite.
   * 
   * @param labels
   *          the labels
   * @param opts
   *          the opts
   * @param p
   *          the p
   * @param callback
   *          the callback
   * @return the radio composite
   */
  public RadioComposite addRadioComposite( String[] labels, int opts, final Object p, final Runnable callback ) {
    RadioComposite rc = new RadioComposite( props, baseStepMeta, composite, opts );
    RowLayout layout = new RowLayout();
    rc.setLayout( layout );
    layout.spacing = 10;
    if ( callback != null ) {
      rc.addListener( new Listener() {
        @Override
        public void handleEvent( Event event ) {
          callback.run();
        }
      } );
    }
    rc.addButtons( labels );
    add( rc );
    return rc;
  }

  public Label addLabel( String label ) {
    Label lbl = new Label( composite, SWT.LEFT );
    lbl.setText( BaseMessages.getString( TeraDataBulkLoaderDialog.PKG, label ) );
    props.setLook( lbl );
    add( lbl );
    return lbl;
  }

  /**
   * Sets the visible.
   * 
   * @param b
   *          the new visible
   */
  public void setVisible( boolean b ) {
    label.setVisible( b );
    composite.setVisible( b );
  }
}
