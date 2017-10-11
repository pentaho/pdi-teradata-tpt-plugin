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
