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
import java.util.Random;

import org.pentaho.di.trans.steps.loadsave.validator.FieldLoadSaveValidator;

public class IntegerValidator implements FieldLoadSaveValidator<Integer> {
  private final Random random = new Random();

  @Override
  public Integer getTestObject() {
    return random.nextInt();
  }

  @Override
  public boolean validateTestObject( Integer testObject, Object actual ) {
    return testObject.equals( actual );
  }
}
