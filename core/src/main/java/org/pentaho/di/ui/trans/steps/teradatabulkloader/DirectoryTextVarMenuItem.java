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

import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.widget.TextVar;

public class DirectoryTextVarMenuItem extends TextVarMenuItem {
  private final Shell shell;

  DirectoryTextVarMenuItem( Shell shell, Composite parent, PropsUI props, TransMeta transMeta, ModifyListener lsMod,
      Control top, String labelProp ) {
    super( parent, props, transMeta, lsMod, top, labelProp, TeraDataBulkLoaderDialog.DIR_BUTTON );
    this.shell = shell;
  }

  DirectoryTextVarMenuItem( Shell shell, Composite parent, PropsUI props, TransMeta transMeta, ModifyListener lsMod,
      CompositeMenuItem top, String labelProp ) {
    this( shell, parent, props, transMeta, lsMod, top.getComposite(), labelProp );
  }

  DirectoryTextVarMenuItem( Shell shell, Composite parent, PropsUI props, TransMeta transMeta, ModifyListener lsMod,
      TextVarMenuItem top, String labelProp ) {
    this( shell, parent, props, transMeta, lsMod, top.getButton(), labelProp );
  }

  @Override
  protected SelectionListener createSelectionListener( TextVar textVar ) {
    return new DirectorySelectionButtonListener( textVar, shell );
  }
}
