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
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.ui.core.PropsUI;

/**
 * The Class RadioComposite.
 */
class RadioComposite extends Composite {
  private final PropsUI props;

  private final BaseStepMeta baseStepMeta;

  /** The ls action type. */
  private Listener lsActionType;

  /** The ls passed listener. */
  private final List<Listener> listeners = new ArrayList<Listener>();

  /**
   * Instantiates a new radio composite.
   * 
   * @param props
   *          the PropsUI object
   * @param changedFlag
   *          the object that will be flagged with any changes
   * @param parent
   *          the parent
   * @param opts
   *          the opts
   */
  RadioComposite( PropsUI props, BaseStepMeta baseStepMeta, Composite parent, int opts ) {
    super( parent, opts );
    this.props = props;
    this.baseStepMeta = baseStepMeta;
    lsActionType = new Listener() {
      @Override
      public void handleEvent( Event event ) {
        Control[] children = getChildren();
        for ( int j = 0; j < children.length; j++ ) {
          Control child = children[j];
          if ( child instanceof Button ) {
            Button bc = (Button) child;
            bc.setSelection( false );
          }
        }
        Button button = (Button) event.widget;
        button.setSelection( true );
        RadioComposite.this.baseStepMeta.setChanged();
      }
    };
  }

  /**
   * Adds the listener.
   * 
   * @param ls
   *          the ls
   */
  public void addListener( Listener ls ) {
    listeners.add( ls );
  }

  /**
   * Adds the buttons.
   * 
   * @param labels
   *          the labels
   */
  public void addButtons( String[] labels ) {
    for ( int i = 0; i < labels.length; i++ ) {
      this.addButton( labels[i] );
    }
  }

  /**
   * Adds the button.
   * 
   * @param label
   *          the label
   */
  public void addButton( String label ) {
    Button button = new Button( this, SWT.RADIO | SWT.RIGHT );
    button.setText( label );
    props.setLook( button );
    button.addListener( SWT.Selection, lsActionType );
    button.addListener( SWT.Selection, new Listener() {

      @Override
      public void handleEvent( Event arg0 ) {
        for ( Listener listener : listeners ) {
          listener.handleEvent( arg0 );
        }
      }
    } );
  }

  /**
   * Gets the selection.
   * 
   * @return the selection
   */
  public int getSelection() {
    Control[] children = getChildren();
    for ( int j = 0; j < children.length; j++ ) {
      Control child = children[j];
      if ( child instanceof Button ) {
        Button bc = (Button) child;
        if ( bc.getSelection() ) {
          return j;
        }
      }
    }
    return 0;
  }

  /**
   * Sets the selection.
   * 
   * @param index
   *          the new selection
   */
  public void setSelection( int index ) {
    Control[] children = getChildren();
    for ( int j = 0; j < children.length; j++ ) {
      Control child = children[j];
      if ( child instanceof Button ) {
        Button bc = (Button) child;
        if ( j == index ) {
          bc.setSelection( true );
        } else {
          bc.setSelection( false );
        }
      }
    }
    Event event = new Event();
    notifyListeners( SWT.SELECTED, event );
    for ( Listener listener : listeners ) {
      listener.handleEvent( event );
    }
  }

  public void setCallback( final Runnable callback ) {
    addListener( new Listener() {
      @Override
      public void handleEvent( Event event ) {
        callback.run();
      }
    } );
  }
}
