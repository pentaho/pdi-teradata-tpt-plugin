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
