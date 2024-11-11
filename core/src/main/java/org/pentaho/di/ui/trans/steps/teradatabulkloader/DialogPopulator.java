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

import java.util.List;

import org.pentaho.di.trans.steps.teradatabulkloader.TeraDataBulkLoaderMeta;

public interface DialogPopulator {
  public void populateMeta( TeraDataBulkLoaderMeta output );

  public void populateDialog( TeraDataBulkLoaderMeta input );

  public void validate( List<String> errors );
}
