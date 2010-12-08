ODBC driver for RDBI
====================

This gem gives you the ability to query ODBC connections with RDBI.

Usage
-----

    > gem install rdbi-driver-odbc
    > irb
    > require 'rdbi-driver-odbc'
    > dbh = RDBI.connect :ODBC, :db => "MY_DSN", :user => "USERNAME",
    *   :password => "PASSWORD"
    > rs = dbh.execute "SELECT * FROM MY_TABLE"
    > rs.as(:Struct).fetch(:first)
  

Copyright
---------

Copyright (c) 2010 Shane Emmons. See LICENSE for details.
