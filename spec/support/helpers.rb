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
    "DROP TABLE IF EXISTS TB2;",
    "CREATE TABLE TB2 (COL1 DATE, COL2 DATETIME, COL3 TIMESTAMP, COL4 TIME);",
    "INSERT INTO TB2 (COL1, COL2, COL3, COL4) VALUES ('2010-01-01', '2010-01-01 12:00:00', '2010-01-01 12:00:00', '12:00:00');",
    "DROP TABLE IF EXISTS TB3;",
    "CREATE TABLE TB3 (COL1 INTEGER PRIMARY KEY);",
  ]

  def init_database
    dbh = new_database
    SQL.each{|q| dbh.execute(q)}
    return dbh
  end
end
