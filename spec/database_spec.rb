require File.expand_path("../spec_helper", __FILE__)
require 'rdbi-driver-odbc'

describe "RDBI::Driver::ODBC::Database" do
  let(:dbh) { init_database }
  subject   { dbh }

  after :each do
    dbh.disconnect if dbh and dbh.connected?
  end

  it { should_not be_nil }
  it { should be_a RDBI::Driver::ODBC::Database }
  it { should be_a RDBI::Database }

  its(:database_name) { should == "testdb" }

  specify "#disconnect" do
    dbh.connected?.should be_true

    dbh.disconnect
    dbh.connected?.should be_false
  end

  specify "#transaction" do
    expect{
      dbh.transaction{
        dbh.execute "INSERT INTO TB3 VALUES (1)"
      }
    }.to_not raise_error

    expect{
      dbh.transaction{
        dbh.execute "INSERT INTO TB3 VALUES (0)"
        dbh.execute "INSERT INTO TB3 VALUES (1)"
      }
    }.to raise_error
  end

  specify "#rollback" do
    expect{dbh.rollback}.to_not raise_error
  end

  specify "#commit" do
    expect{dbh.commit}.to_not raise_error
  end

  specify "#new_statement" do
    stmt = dbh.new_statement("SELECT * FROM TBL")
    stmt.should be_an RDBI::Driver::ODBC::Statement
    stmt.finish
  end

  describe "#table_schema" do
    context "TB1" do
      let(:ts) { dbh.table_schema "TB1" }
      subject  { ts }

      it { should be_a RDBI::Schema }

      its(:tables) { should == ["TB1"] }

      context "ts[:columns][0]" do
        subject { ts[:columns][0] }

        its(:name)        { should == :COL1    }
        its(:type)        { should == "CHAR"   }
        its(:ruby_type)   { should == :default }
        its(:precision)   { should == 0        }
        its(:scale)       { should == 0        }
        its(:nullable)    { should == true     }
        its(:table)       { should == "TB1"    }
        its(:primary_key) { should == false    }
      end

      context "ts[:columns][1]" do
        subject { ts[:columns][1] }

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

    context "TB3" do
      let(:ts) { dbh.table_schema "TB3" }
      subject  { ts }

      it { should be_a RDBI::Schema }

      its(:tables) { should == ["TB3"] }

      context "ts[:columns][0]" do
        subject { ts[:columns][0] }

        its(:name)        { should == :COL1     }
        its(:type)        { should == "INTEGER" }
        its(:ruby_type)   { should == :integer  }
        its(:precision)   { should == 10        }
        its(:scale)       { should == 0         }
        its(:nullable)    { should == false     }
        its(:table)       { should == "TB3"     }
        its(:primary_key) { should == true      }
      end
    end

  end

  describe "#schema" do
    let(:ts) { dbh.schema }
    subject  { ts }

    it {should be_an Array }

    its(:length) { should == 3 }

    context "ts[0]" do
      let(:t) { ts[0] }
      subject { t }

      it { should be_a RDBI::Schema }

      its(:tables) { should == ["tb1"] }

      context "t[:columns][0]" do
        subject { t[:columns][0] }

        its(:name)        { should == :COL1    }
        its(:type)        { should == "CHAR"   }
        its(:ruby_type)   { should == :default }
        its(:precision)   { should == 0        }
        its(:scale)       { should == 0        }
        its(:nullable)    { should == true     }
        its(:table)       { should == "tb1"    }
        its(:primary_key) { should == false    }
      end

      context "t[:columns][1]" do
        subject { t[:columns][1] }

        its(:name)        { should == :COL2     }
        its(:type)        { should == "INTEGER" }
        its(:ruby_type)   { should == :integer  }
        its(:precision)   { should == 10        }
        its(:scale)       { should == 0         }
        its(:nullable)    { should == true      }
        its(:table)       { should == "tb1"     }
        its(:primary_key) { should == false     }
      end
    end

    context "ts[1]" do
      let(:t) { ts[1] }
      subject { t }

      it { should be_a RDBI::Schema }

      its(:tables) { should == ["tb2"] }

      context "t[:columns][0]" do
        subject { t[:columns][0] }

        its(:name)        { should == :COL1  }
        its(:type)        { should == "DATE" }
        its(:ruby_type)   { should == :date  }
        its(:precision)   { should == 10     }
        its(:scale)       { should == 0      }
        its(:nullable)    { should == true   }
        its(:table)       { should == "tb2"  }
        its(:primary_key) { should == false  }
      end

      context "t[:columns][1]" do
        subject { t[:columns][1] }

        its(:name)        { should == :COL2       }
        its(:type)        { should == "TIMESTAMP" }
        its(:ruby_type)   { should == :timestamp  }
        its(:precision)   { should == 19          }
        its(:scale)       { should == 0           }
        its(:nullable)    { should == true        }
        its(:table)       { should == "tb2"       }
        its(:primary_key) { should == false       }
      end

      context "t[:columns][2]" do
        subject { t[:columns][2] }

        its(:name)        { should == :COL3       }
        its(:type)        { should == "TIMESTAMP" }
        its(:ruby_type)   { should == :timestamp  }
        its(:precision)   { should == 19          }
        its(:scale)       { should == 0           }
        its(:nullable)    { should == true        }
        its(:table)       { should == "tb2"       }
        its(:primary_key) { should == false       }
      end

      context "t[:columns][3]" do
        subject { t[:columns][3] }

        its(:name)        { should == :COL4  }
        its(:type)        { should == "TIME" }
        its(:ruby_type)   { should == :time  }
        its(:precision)   { should == 8      }
        its(:scale)       { should == 0      }
        its(:nullable)    { should == true   }
        its(:table)       { should == "tb2"  }
        its(:primary_key) { should == false  }
      end
    end

    context "ts[2]" do
      let(:t) { ts[2] }
      subject { t }

      it { should be_a RDBI::Schema }

      its(:tables) { should == ["tb3"] }

      context "t[:columns][0]" do
        subject { t[:columns][0] }

        its(:name)        { should == :COL1     }
        its(:type)        { should == "INTEGER" }
        its(:ruby_type)   { should == :integer  }
        its(:precision)   { should == 10        }
        its(:scale)       { should == 0         }
        its(:nullable)    { should == false     }
        its(:table)       { should == "tb3"     }
        its(:primary_key) { should == true      }
      end
    end
  end

  specify "#ping" do
    dbh.ping.should be_true

    dbh.disconnect
    dbh.ping.should be_false
  end

  specify "#quote" do
    dbh.quote(1).should        == "1"
    dbh.quote(1.2).should      == "1.2"
    dbh.quote("string").should == "'string'"
    dbh.quote(nil).should      == "NULL"
    dbh.quote(true).should     == "1"
    dbh.quote(false).should    == "0"
  end
end
