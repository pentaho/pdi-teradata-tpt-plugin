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
package org.pentaho.di.trans.steps.teradatabulkloader;
import org.pentaho.di.trans.steps.loadsave.validator.ArrayLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.FieldLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.StringLoadSaveValidator;

public class StringArrayLoadSaveValidator implements FieldLoadSaveValidator<String[]> {
  private final FieldLoadSaveValidator<String[]> delegate = new ArrayLoadSaveValidator<String>(
      new StringLoadSaveValidator() );
  private final String[] testObject = delegate.getTestObject();

  @Override
  public boolean validateTestObject( String[] arg0, Object arg1 ) {
    return delegate.validateTestObject( arg0, arg1 );
  }

  @Override
  public String[] getTestObject() {
    return testObject;
  }
}
