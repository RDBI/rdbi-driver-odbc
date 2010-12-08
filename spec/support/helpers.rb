require 'rdbi-dbrc'

module HelperMethods

  def new_database
    RDBI::DBRC.connect(:odbc_testdb)
  end

  SQL = [
    "DROP TABLE IF EXISTS TB1;",
    "CREATE TABLE TB1 (COL1 CHAR(1), COL2 INTEGER);",
    "INSERT INTO TB1 (COL1, COL2) VALUES ('A', 1);",
    "INSERT INTO TB1 (COL1, COL2) VALUES ('B', 2);",
    "INSERT INTO TB1 (COL1, COL2) VALUES ('C', 3);",
  ]

  def init_database
    dbh = new_database
    SQL.each{|q| dbh.execute(q)}
    return dbh
  end
end
