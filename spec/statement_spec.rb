require File.expand_path("../spec_helper", __FILE__)
require 'rdbi-driver-odbc'

describe "RDBI::Driver::ODBC::Statement" do
  before :each do
    @dbh = init_database
    @sth = @dbh.new_statement("SELECT * FROM TB1 WHERE COL1 = ?")
  end

  after :each do
    @sth.finish if @sth and not @sth.finished?
    @dbh.disconnect if @dbh and @dbh.connected?
  end

  specify "#new" do
    @sth.should_not be_nil
    @sth.should be_an RDBI::Driver::ODBC::Statement
    @sth.should be_an RDBI::Statement
  end

  specify "#new_execution" do
    rs = @sth.new_execution("A")
    rs.should_not be_nil
    rs.should be_an Array
    rs[0].should be_an RDBI::Driver::ODBC::Cursor
    rs[1].should be_an RDBI::Schema
    rs[2].should be_an Hash

    rs[1][:columns][0][:name].should == :COL1
    rs[1][:columns][0][:type].should == "CHAR"
    rs[1][:columns][0][:ruby_type].should == :default
    rs[1][:columns][0][:precision].should == 0
    rs[1][:columns][0][:scale].should == 0
    rs[1][:columns][0][:nullable].should == true
    rs[1][:columns][0][:table].should == "TB1"

    rs[1][:columns][1][:name].should == :COL2
    rs[1][:columns][1][:type].should == "INTEGER"
    rs[1][:columns][1][:ruby_type].should == :integer
    rs[1][:columns][1][:precision].should == 10
    rs[1][:columns][1][:scale].should == 0
    rs[1][:columns][1][:nullable].should == true
    rs[1][:columns][1][:table].should == "TB1"
  end

  specify "#execute" do
    rs = @sth.execute("A")
    rs.should_not be_nil
    rs.should be_an RDBI::Result

    r = rs.fetch(:first)
    r[0].should == "A"
    r[1].should == 1

    r = rs.as(:Struct).fetch(:first)
    r[:COL1].should == "A"
    r[:COL2].should == 1
  end

  specify "#finish" do
    @sth.finished?.should be_false
    @sth.finish
    @sth.finished?.should be_true
  end

  describe "@output_type_map[:date]" do
    before :each do
      @rs = @dbh.execute("SELECT COL1 FROM TB2")
    end

    specify "::ODBC::Date" do
      r = @rs.as(:Struct).fetch(:first)
      r[:COL1].should be_a Date
      r[:COL1].should == Date.parse("2010-01-01")
    end
  end

  describe "@output_type_map[:datetime]" do
    before :each do
      @rs = @dbh.execute("SELECT COL2, COL3 FROM TB2")
    end

    specify "::ODBC::TimeStamp" do
      r = @rs.as(:Struct).fetch(:first)
      r[:COL2].should be_a DateTime
      r[:COL3].should be_a DateTime
      r[:COL2].should == DateTime.parse("2010-01-01 12:00:00")
      r[:COL3].should == DateTime.parse("2010-01-01 12:00:00")
    end
  end

  describe "@output_type_map[:time]" do
    before :each do
      @rs = @dbh.execute("SELECT COL4 FROM TB2")
    end

    specify "::ODBC::Time" do
      r = @rs.as(:Struct).fetch(:first)
      r[:COL4].should be_a Time
      r[:COL4].should == Time.parse("12:00:00")
    end
  end
end
