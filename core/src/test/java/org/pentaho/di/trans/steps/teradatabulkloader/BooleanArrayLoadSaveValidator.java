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
import org.pentaho.di.trans.steps.loadsave.validator.BooleanLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.FieldLoadSaveValidator;

public class BooleanArrayLoadSaveValidator implements FieldLoadSaveValidator<Boolean[]> {
  private final BooleanLoadSaveValidator delegate = new BooleanLoadSaveValidator();
  private final ArrayLoadSaveValidator<Boolean> validator = new ArrayLoadSaveValidator<Boolean>(
      new BooleanLoadSaveValidator() );
  private final int length;

  public BooleanArrayLoadSaveValidator( int length ) {
    this.length = length;
  }

  @Override
  public Boolean[] getTestObject() {
    Boolean[] result = new Boolean[length];
    for ( int i = 0; i < length; i++ ) {
      result[i] = delegate.getTestObject();
    }
    return result;
  }

  @Override
  public boolean validateTestObject( Boolean[] testObject, Object actual ) {
    return validator.validateTestObject( testObject, actual );
  }

}
