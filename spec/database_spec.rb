require File.expand_path("../spec_helper", __FILE__)
require 'rdbi-driver-odbc'

describe "RDBI::Driver::ODBC::Database" do
  before :each do
    @dbh = init_database
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
    lambda{
      @dbh.transaction{
        @dbh.execute "INSERT INTO TB3 VALUES (1)"
      }
    }.should_not raise_error

    lambda{
      @dbh.transaction{
        @dbh.execute "INSERT INTO TB3 VALUES (0)"
        @dbh.execute "INSERT INTO TB3 VALUES (1)"
      }
    }.should raise_error
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
    stmt.finish
  end

  specify "#table_schema" do
    ts = @dbh.table_schema("TB1")
    ts.should be_a RDBI::Schema

    ts[:columns][0][:name].should == :COL1
    ts[:columns][0][:type].should == "CHAR"
    ts[:columns][0][:ruby_type].should == :default
    ts[:columns][0][:precision].should == 0
    ts[:columns][0][:scale].should == 0
    ts[:columns][0][:nullable].should == true
    ts[:columns][0][:table].should == "TB1"
    ts[:columns][0][:primary_key].should == false

    ts[:columns][1][:name].should == :COL2
    ts[:columns][1][:type].should == "INTEGER"
    ts[:columns][1][:ruby_type].should == :integer
    ts[:columns][1][:precision].should == 10
    ts[:columns][1][:scale].should == 0
    ts[:columns][1][:nullable].should == true
    ts[:columns][1][:table].should == "TB1"
    ts[:columns][1][:primary_key].should == false

    ts[:tables].should == ["TB1"]

    ts = @dbh.table_schema("TB3")
    ts.should be_a RDBI::Schema

    ts[:columns][0][:name].should == :COL1
    ts[:columns][0][:type].should == "INTEGER"
    ts[:columns][0][:ruby_type].should == :integer
    ts[:columns][0][:precision].should == 10
    ts[:columns][0][:scale].should == 0
    ts[:columns][0][:nullable].should == false
    ts[:columns][0][:table].should == "TB3"
    ts[:columns][0][:primary_key].should == true

    ts[:tables].should == ["TB3"]
  end

  specify "#schema" do
    ts = @dbh.schema
    ts.should be_an Array
    ts.length.should == 3

    ts[0].should be_a RDBI::Schema
    ts[0][:tables].should == ["tb1"]

    ts[0][:columns][0][:name].should == :COL1
    ts[0][:columns][0][:type].should == "CHAR"
    ts[0][:columns][0][:ruby_type].should == :default
    ts[0][:columns][0][:precision].should == 0
    ts[0][:columns][0][:scale].should == 0
    ts[0][:columns][0][:nullable].should == true
    ts[0][:columns][0][:table].should == "tb1"
    ts[0][:columns][0][:primary_key].should == false

    ts[0][:columns][1][:name].should == :COL2
    ts[0][:columns][1][:type].should == "INTEGER"
    ts[0][:columns][1][:ruby_type].should == :integer
    ts[0][:columns][1][:precision].should == 10
    ts[0][:columns][1][:scale].should == 0
    ts[0][:columns][1][:nullable].should == true
    ts[0][:columns][1][:table].should == "tb1"
    ts[0][:columns][1][:primary_key].should == false

    ts[1].should be_a RDBI::Schema
    ts[1][:tables].should == ["tb2"]

    ts[1][:columns][0][:name].should == :COL1
    ts[1][:columns][0][:type].should == "DATE"
    ts[1][:columns][0][:ruby_type].should == :date
    ts[1][:columns][0][:precision].should == 10
    ts[1][:columns][0][:scale].should == 0
    ts[1][:columns][0][:nullable].should == true
    ts[1][:columns][0][:table].should == "tb2"
    ts[1][:columns][0][:primary_key].should == false

    ts[1][:columns][1][:name].should == :COL2
    ts[1][:columns][1][:type].should == "TIMESTAMP"
    ts[1][:columns][1][:ruby_type].should == :timestamp
    ts[1][:columns][1][:precision].should == 19
    ts[1][:columns][1][:scale].should == 0
    ts[1][:columns][1][:nullable].should == true
    ts[1][:columns][1][:table].should == "tb2"
    ts[1][:columns][1][:primary_key].should == false

    ts[1][:columns][2][:name].should == :COL3
    ts[1][:columns][2][:type].should == "TIMESTAMP"
    ts[1][:columns][2][:ruby_type].should == :timestamp
    ts[1][:columns][2][:precision].should == 19
    ts[1][:columns][2][:scale].should == 0
    ts[1][:columns][2][:nullable].should == true
    ts[1][:columns][2][:table].should == "tb2"
    ts[1][:columns][2][:primary_key].should == false

    ts[1][:columns][3][:name].should == :COL4
    ts[1][:columns][3][:type].should == "TIME"
    ts[1][:columns][3][:ruby_type].should == :time
    ts[1][:columns][3][:precision].should == 8
    ts[1][:columns][3][:scale].should == 0
    ts[1][:columns][3][:nullable].should == true
    ts[1][:columns][3][:table].should == "tb2"
    ts[1][:columns][3][:primary_key].should == false

    ts[2].should be_a RDBI::Schema
    ts[2][:tables].should == ["tb3"]

    ts[2][:columns][0][:name].should == :COL1
    ts[2][:columns][0][:type].should == "INTEGER"
    ts[2][:columns][0][:ruby_type].should == :integer
    ts[2][:columns][0][:precision].should == 10
    ts[2][:columns][0][:scale].should == 0
    ts[2][:columns][0][:nullable].should == false
    ts[2][:columns][0][:table].should == "tb3"
    ts[2][:columns][0][:primary_key].should == true
  end

  specify "#ping" do
    @dbh.ping.should be_true
    @dbh.disconnect
    @dbh.ping.should be_false
  end

  specify "#quote" do
    @dbh.quote(1).should        == "1"
    @dbh.quote(1.2).should      == "1.2"
    @dbh.quote("string").should == "'string'"
    @dbh.quote(nil).should      == "NULL"
    @dbh.quote(true).should     == "1"
    @dbh.quote(false).should    == "0"
  end
end
