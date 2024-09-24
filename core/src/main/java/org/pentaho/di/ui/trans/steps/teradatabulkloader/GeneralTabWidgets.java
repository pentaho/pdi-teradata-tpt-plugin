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

import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.widgets.Button;

public class GeneralTabWidgets {
  private final Button randomizeFifoButton;

  private final CTabItem tab;

  public GeneralTabWidgets( CTabItem tab, Button randomizeFifoButton ) {
    this.tab = tab;
    this.randomizeFifoButton = randomizeFifoButton;
  }
  public Button getRandomizeFifoButton() {
    return randomizeFifoButton;
  }

  public CTabItem getTab() {
    return tab;
  }
}
