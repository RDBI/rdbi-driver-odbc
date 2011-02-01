require File.expand_path("../spec_helper", __FILE__)
require 'rdbi-driver-odbc'

describe "RDBI::Driver::ODBC::Statement" do
  let(:dbh) { init_database }
  let(:sth) { dbh.new_statement "SELECT * FROM TB1 WHERE COL1 = ?" }
  subject   { sth }

  after :each do
    sth.finish if sth and not sth.finished?
    dbh.disconnect if dbh and dbh.connected?
  end

  it { should_not be_nil }
  it { should be_a RDBI::Driver::ODBC::Statement }
  it { should be_a RDBI::Statement }

  describe "#new_execution" do
    let(:rs) { sth.new_execution "A" }
    subject  { rs }

    it { should_not be_nil }
    it { should be_an Array }

    specify "it should return the correct types" do
      rs[0].should be_an RDBI::Driver::ODBC::Cursor
      rs[1].should be_an RDBI::Schema
      rs[2].should be_an Hash

      rs[1][:tables].should == ["TB1"]
    end

    context "rs[1][:columns][0]" do
      subject { rs[1][:columns][0] }

      its(:name)        { should == :COL1    }
      its(:type)        { should == "CHAR"   }
      its(:ruby_type)   { should == :default }
      its(:precision)   { should == 0        }
      its(:scale)       { should == 0        }
      its(:nullable)    { should == true     }
      its(:table)       { should == "TB1"    }
      its(:primary_key) { should == false    }
    end

    context "rs[1][:columns[1]" do
      subject { rs[1][:columns][1] }

      its(:name)        { should == :COL2     }
      its(:type)        { should == "INTEGER" }
      its(:ruby_type)   { should == :integer  }
      its(:precision)   { should == 10        }
      its(:scale)       { should == 0         }
      its(:nullable)    { should == true      }
      its(:table)       { should == "TB1"     }
      its(:primary_key) { should == false     }
    end
  end

  describe "#execute" do
    let(:rs) { sth.execute("A") }
    subject  { rs }

    it { should_not be_nil }
    it { should be_an RDBI::Result }

    it "should return correct results" do
      r = rs.fetch(:first)
      r[0].should == "A"
      r[1].should == 1

      r = rs.as(:Struct).fetch(:first)
      r[:COL1].should == "A"
      r[:COL2].should == 1
    end
  end

  specify "#finish" do
    sth.finished?.should be_false

    sth.finish
    sth.finished?.should be_true
  end

  context "@output_type_map[:date]" do
    let(:rs) { dbh.execute "SELECT COL1 FROM TB2" }
    let(:r)  { rs.as(:Struct).fetch(:first) }

    specify "::ODBC::Date" do
      r[:COL1].should be_a Date
      r[:COL1].should == Date.parse("2010-01-01")
    end
  end

  context "@output_type_map[:datetime]" do
    let(:rs) { dbh.execute "SELECT COL2, COL3 FROM TB2" }
    let(:r)  { rs.as(:Struct).fetch(:first) }

    specify "::ODBC::TimeStamp" do
      r[:COL2].should be_a DateTime
      r[:COL3].should be_a DateTime
      r[:COL2].should == DateTime.parse("2010-01-01 12:00:00")
      r[:COL3].should == DateTime.parse("2010-01-01 12:00:00")
    end
  end

  context "@output_type_map[:time]" do
    let(:rs) { dbh.execute "SELECT COL4 FROM TB2" }
    let(:r)  { rs.as(:Struct).fetch(:first) }

    specify "::ODBC::Time" do
      r[:COL4].should be_a Time
      r[:COL4].should == Time.parse("12:00:00")
    end
  end
end
