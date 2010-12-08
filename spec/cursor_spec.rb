require File.expand_path("../spec_helper", __FILE__)
require 'rdbi-driver-odbc'

describe "RDBI::Driver::ODBC::Cursor" do
  before :each do
    @dbh = init_database
    @sth = @dbh.new_statement("SELECT * FROM TB1")
    @cur = @sth.new_execution[0]
  end

  after :each do
    @sth.finish if @sth and not @sth.finished?
    @dbh.disconnect if @dbh and @dbh.connected?
  end

  specify "#new" do
    @cur.should_not be_nil
    @cur.should be_an RDBI::Driver::ODBC::Cursor
    @cur.should be_an RDBI::Cursor
  end

  specify "#next_row" do
    @cur.next_row.should == ["A", 1]
    @cur.next_row.should == ["B", 2]
    @cur.next_row.should == ["C", 3]
    @cur.next_row.should == nil
  end

  specify "#result_count" do
    @cur.result_count.should == 3
  end

  specify "#affected_count" do
    @cur.affected_count.should == 0
  end

  specify "#first" do
    @cur.first.should == ["A", 1]
  end

  specify "#last" do
    @cur.last.should == ["C", 3]
  end

  specify "#rest" do
    @cur.rest.should == [
      ["A", 1],
      ["B", 2],
      ["C", 3],
    ]

    @cur.next_row
    @cur.rest.should == [
      ["B", 2],
      ["C", 3],
    ]
  end

  specify "#all" do
    @cur.all.should == [
      ["A", 1],
      ["B", 2],
      ["C", 3],
    ]
  end

  specify "#fetch" do
    @cur.fetch.should == [["A", 1]]
    @cur.fetch(2).should == [
      ["A", 1],
      ["B", 2],
    ]
    @cur.fetch(3).should == [
      ["A", 1],
      ["B", 2],
      ["C", 3],
    ]
    @cur.fetch(4).should == [
      ["A", 1],
      ["B", 2],
      ["C", 3],
    ]
    3.times{@cur.next_row}
    @cur.fetch.should == []
  end

  specify "#[]" do
    @cur[0].should == ["A", 1]
    @cur[1].should == ["B", 2]
    @cur[2].should == ["C", 3]
    @cur[3].should == nil
    @cur[-1].should == ["C", 3]
  end

  specify "#last_row?" do
    @cur.last_row?.should be_false
    3.times{@cur.next_row}
    @cur.last_row?.should be_true
  end

  specify "#empty?" do
    @cur.empty?.should be_false
    @sth.finish
    @sth = @dbh.new_statement("SELECT * FROM TB1 WHERE COL1 = 'D'")
    @cur = @sth.new_execution[0]
    @cur.empty?.should be_true
  end

  specify "#rewind" do
    @cur.next_row.should == ["A", 1]
    @cur.next_row.should == ["B", 2]
    @cur.rewind
    @cur.next_row.should == ["A", 1]
  end

  specify "#size" do
    @cur.size.should == 3
  end

  specify "#finish" do
    lambda{@cur.finish}.should_not raise_error
  end

  specify "#coerce_to_array" do
    @cur.coerce_to_array.should == [
      ["A", 1],
      ["B", 2],
      ["C", 3],
    ]
  end
end
