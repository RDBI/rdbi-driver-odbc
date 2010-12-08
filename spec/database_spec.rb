require File.expand_path("../spec_helper", __FILE__)
require 'rdbi-driver-odbc'

describe "RDBI::Driver::ODBC::Database" do
  before :each do
    @dbh = new_database
  end

  after :each do
    @dbh.disconnect if @dbh and @dbh.connected?
  end

  specify "#new" do
    @dbh.should_not be_nil
    @dbh.should be_an RDBI::Driver::ODBC::Database
    @dbh.should be_an RDBI::Database
    @dbh.database_name.should == "testdb"
  end

  specify "#disconnect" do
    @dbh.connected?.should be_true
    @dbh.disconnect
    @dbh.connected?.should be_false
  end

  specify "#transaction" do
    lambda{@dbh.transaction{}}.should raise_error NotImplementedError
  end

  specify "#rollback" do
    lambda{@dbh.rollback}.should_not raise_error
  end

  specify "#commit" do
    lambda{@dbh.commit}.should_not raise_error
  end

  specify "#new_statement" do
    stmt = @dbh.new_statement("SELECT * FROM TBL")
    stmt.should be_an RDBI::Driver::ODBC::Statement
  end

  specify "#table_schema" do
    lambda{@dbh.table_schema("TBL")}.should raise_error NotImplementedError
  end

  specify "#schema" do
    lambda{@dbh.schema}.should raise_error NotImplementedError
  end

  specify "#ping" do
    @dbh.ping.should be_true
    @dbh.disconnect
    @dbh.ping.should be_false
  end
end
