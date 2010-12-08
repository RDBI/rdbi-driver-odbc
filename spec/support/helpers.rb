require 'rdbi-dbrc'

module HelperMethods

  def new_database
    RDBI::DBRC.connect(:odbc_testdb)
  end

  def init_database
    dbh = new_database
    return dbh
  end
end
