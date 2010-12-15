RDBI-DRIVER-ODBC 0.1.2
======================

Features
--------
 - Fix to work with RDBI v0.9.1
 - Update rspec development dependency to ~> 2

RDBI-DRIVER-ODBC 0.1.1
======================

Features
--------
 - Add specs for all classes
 - Update RDBI::Column information
 - Automatically convert ::ODBC::Date, ::ODBC::Time and ::ODBC::TimeStamp
   objects to ::Date, ::Time, ::TimeStamp

Bugfixes
--------
 - Call #super at end of Statement#finish to cleanup
 - Return nil when Cursor#next_row is called and no more rows to fetch
 - Return +all+ remaning rows from Cursor#rest


RDBI-DRIVER-ODBC 0.1.0
======================

Features
--------
 - Initial release. Please experiment and report and bugs you find.
