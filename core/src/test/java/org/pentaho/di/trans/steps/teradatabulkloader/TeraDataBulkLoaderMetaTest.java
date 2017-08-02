package org.pentaho.di.trans.steps.teradatabulkloader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.steps.loadsave.LoadSaveTester;
import org.pentaho.di.trans.steps.loadsave.validator.FieldLoadSaveValidator;

public class TeraDataBulkLoaderMetaTest {
  @Test
  public void testLoadSaveRoundTrip() throws KettleException {
    // General
    List<String> attributes =
        new ArrayList<String>( Arrays.asList( TeraDataBulkLoaderMeta.GENERATE_SCRIPT_FIELD,
            TeraDataBulkLoaderMeta.TBUILD_PATH_FIELD, TeraDataBulkLoaderMeta.TBUILD_LIB_PATH_FIELD,
            TeraDataBulkLoaderMeta.LIB_PATH_FIELD, TeraDataBulkLoaderMeta.TDICU_LIB_PATH_FIELD,
            TeraDataBulkLoaderMeta.COP_LIB_PATH_FIELD, TeraDataBulkLoaderMeta.TWB_ROOT_FIELD,
            TeraDataBulkLoaderMeta.INSTALL_PATH_FIELD, TeraDataBulkLoaderMeta.JOB_NAME_FIELD ) );

    // Generated script
    attributes.addAll( Arrays.asList( TeraDataBulkLoaderMeta.SCHEMA_FIELD, TeraDataBulkLoaderMeta.TABLE_FIELD,
        TeraDataBulkLoaderMeta.LOG_TABLE_FIELD, TeraDataBulkLoaderMeta.WORK_TABLE_FIELD,
        TeraDataBulkLoaderMeta.ERROR_TABLE_FIELD, TeraDataBulkLoaderMeta.ERROR_TABLE_2_FIELD,
        TeraDataBulkLoaderMeta.DROP_LOG_TABLE_FIELD, TeraDataBulkLoaderMeta.DROP_WORK_TABLE_FIELD,
        TeraDataBulkLoaderMeta.DROP_ERROR_TABLE_FIELD, TeraDataBulkLoaderMeta.DROP_ERROR_TABLE_2_FIELD,
        TeraDataBulkLoaderMeta.IGNORE_DUP_UPDATE_FIELD, TeraDataBulkLoaderMeta.INSERT_MISSING_UPDATE_FIELD,
        TeraDataBulkLoaderMeta.IGNORE_MISSING_UPDATE_FIELD, TeraDataBulkLoaderMeta.ACCESS_LOG_FILE_FIELD,
        TeraDataBulkLoaderMeta.UPDATE_LOG_FILE_FIELD, TeraDataBulkLoaderMeta.FIFO_FILE_NAME_FIELD,
        TeraDataBulkLoaderMeta.RANDOMIZE_FIFO_FILE_NAME_FIELD, TeraDataBulkLoaderMeta.SCRIPT_FILE_NAME_FIELD,
        TeraDataBulkLoaderMeta.ACTION_TYPE_FIELD, TeraDataBulkLoaderMeta.KEY_STREAM_FIELD,
        TeraDataBulkLoaderMeta.KEY_LOOKUP_FIELD, TeraDataBulkLoaderMeta.KEY_CONDITION_FIELD,
        TeraDataBulkLoaderMeta.FIELD_TABLE_FIELD, TeraDataBulkLoaderMeta.FIELD_STREAM_FIELD,
        TeraDataBulkLoaderMeta.FIELD_UPDATE_FIELD ) );

    // Use existing
    attributes.addAll( Arrays.asList( TeraDataBulkLoaderMeta.EXISTING_SCRIPT_FILE_FIELD,
        TeraDataBulkLoaderMeta.SUBSTITUTE_SCRIPT_FILE_FIELD, TeraDataBulkLoaderMeta.EXISTING_VARIABLE_FILE_FIELD,
        TeraDataBulkLoaderMeta.SUBSTITUTE_VARIABLE_FILE_FIELD ) );

    // Nonstandard getters
    Map<String, String> getterMap = new HashMap<String, String>();
    getterMap.put( TeraDataBulkLoaderMeta.INSTALL_PATH_FIELD, "getTdInstallPath" );
    getterMap.put( TeraDataBulkLoaderMeta.SCHEMA_FIELD, "getSchemaName" );
    getterMap.put( TeraDataBulkLoaderMeta.TABLE_FIELD, "getTableName" );
    getterMap.put( TeraDataBulkLoaderMeta.SUBSTITUTE_SCRIPT_FILE_FIELD, "getSubstituteControlFile" );
    getterMap.put( TeraDataBulkLoaderMeta.EXISTING_VARIABLE_FILE_FIELD, "getVariableFile" );

    // Nonstandard setters
    Map<String, String> setterMap = new HashMap<String, String>();
    setterMap.put( TeraDataBulkLoaderMeta.INSTALL_PATH_FIELD, "setTdInstallPath" );
    setterMap.put( TeraDataBulkLoaderMeta.SCHEMA_FIELD, "setSchemaName" );
    setterMap.put( TeraDataBulkLoaderMeta.TABLE_FIELD, "setTableName" );
    setterMap.put( TeraDataBulkLoaderMeta.SUBSTITUTE_SCRIPT_FILE_FIELD, "setSubstituteControlFile" );
    setterMap.put( TeraDataBulkLoaderMeta.EXISTING_VARIABLE_FILE_FIELD, "setVariableFile" );

    Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap =
        new HashMap<String, FieldLoadSaveValidator<?>>();

    Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorTypeMap =
        new HashMap<String, FieldLoadSaveValidator<?>>();

    // Custom validators so all arrays will be the same size
    final FieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator = new StringArrayLoadSaveValidator();
    fieldLoadSaveValidatorTypeMap.put( String[].class.getCanonicalName(), stringArrayLoadSaveValidator );
    fieldLoadSaveValidatorAttributeMap.put( TeraDataBulkLoaderMeta.FIELD_UPDATE_FIELD,
        new BooleanArrayLoadSaveValidator( stringArrayLoadSaveValidator.getTestObject().length ) );

    // Needed int validator
    fieldLoadSaveValidatorTypeMap.put( Integer.class.getCanonicalName(), new IntegerValidator() );
    fieldLoadSaveValidatorTypeMap.put( int.class.getCanonicalName(), new IntegerValidator() );

    LoadSaveTester loadSaveTester =
        new LoadSaveTester( TeraDataBulkLoaderMeta.class, attributes, getterMap, setterMap,
            fieldLoadSaveValidatorAttributeMap, fieldLoadSaveValidatorTypeMap );

    loadSaveTester.testRepoRoundTrip();
    loadSaveTester.testXmlRoundTrip();
  }
}
