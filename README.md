pdi-teradata-tpt-plugin - 'Teradata TPT Insert Upsert Bulk Loader'
==================================================================

This step supports bulk loading via TPT using the tbuild command and supports Insert and Upsert. 
It emulates the traditional Teradata MultiLoad utility and should be used instead of the Teradata Fastload bulkloader step.

Status:
- Beta to distribute via the PDI Marketplace (compatible with PDI 4.x and 5.0)
- Within the actual beta and preview state, we offer limited customer support: 
  Assistance is given by Services Development with no contractual support for production environments until PDI 5.1 GA.

History:
- Derived from package org.pentaho.di.trans.steps.terafast
- Modified for TPT by Kevin Hanrahan (cfikevin/pentaho-kettle forked from pentaho/pentaho-kettle)
- Moved the core pentaho-kettle classes into a plugin project pdi-teradata-tpt-plugin (using mattyb149/pentaho-plugin-skeletons)
- Using annotations instead of plugin.xml
- Renamed the step to 'Teradata TPT Insert Upsert Bulk Loader', minor label changes

Notes:
- To build the plug-in jar and to resolve dependencies, use target 'default'
- To create a Marketplace zip, build with target 'package' to create dist/TeraDataBulkLoader.zip (includes package-res/version.xml)

TODO (see also http://jira.pentaho.com/browse/PDI-10216): 
- Code review: remove system.out, print stacktraces etc.
- Code change from PDI 4.x to PDI 5.0/5.1 API (see also actual build warnings) 
- Build: Review the usage of pentaho-plugin-skeletons (icon in package-res), mattyb149 updated pentaho-plugin-skeletons since it was used
- UX: needs changes (tabs look different, some buttons/labels are too narrow) and get it reviewed by UX  
- QA: check if variables are resolved correctly, test save to/from repository etc.
- Doc: Finalize documentation: http://wiki.pentaho.com/display/EAI/Teradata+TPT+Insert+Upsert+Bulk+Loader
- Check maturity level, upload to marketplace