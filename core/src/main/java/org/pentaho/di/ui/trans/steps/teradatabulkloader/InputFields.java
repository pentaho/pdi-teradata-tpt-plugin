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

import java.util.Collections;
import java.util.Map;

public class InputFields {
  /** The input field length. */
  private final Map<String, Integer> inputFieldLength;

  /** The input fields. */
  private final Map<String, Integer> inputFields;

  /** The input field type. */
  private final Map<String, Integer> inputFieldType;

  public InputFields( Map<String, Integer> inputFields, Map<String, Integer> inputFieldType,
      Map<String, Integer> inputFieldLength ) {
    this.inputFields = inputFields;
    this.inputFieldType = inputFieldType;
    this.inputFieldLength = inputFieldLength;
  }

  public Map<String, Integer> getInputFieldLength() {
    return Collections.unmodifiableMap( inputFieldLength );
  }

  public Map<String, Integer> getInputFields() {
    return Collections.unmodifiableMap( inputFields );
  }

  public Map<String, Integer> getInputFieldType() {
    return Collections.unmodifiableMap( inputFieldType );
  }

}
