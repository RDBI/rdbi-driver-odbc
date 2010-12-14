require 'rdbi'
require 'rubygems'
gem 'ruby-odbc', '= 0.99992'
require 'odbc'

class RDBI::Driver::ODBC < RDBI::Driver
  def initialize(*args)
    super Database, *args
  end
end

class RDBI::Driver::ODBC < RDBI::Driver

  SQL_TYPES = {
      1 => {:type => "CHAR",           :ruby_type => :default},
      2 => {:type => "NUMERIC",        :ruby_type => :decimal},
      3 => {:type => "DECIMAL",        :ruby_type => :decimal},
      4 => {:type => "INTEGER",        :ruby_type => :integer},
      5 => {:type => "SMALLINT",       :ruby_type => :integer},
      6 => {:type => "FLOAT",          :ruby_type => :decimal},
      7 => {:type => "REAL",           :ruby_type => :decimal},
      8 => {:type => "DOUBLE",         :ruby_type => :decimal},
      9 => {:type => "DATE",           :ruby_type => :datetime},
     10 => {:type => "TIME",           :ruby_type => :timestamp},
     11 => {:type => "TIMESTAMP",      :ruby_type => :timestamp},
     12 => {:type => "VARCHAR",        :ruby_type => :default},
     13 => {:type => "BOOLEAN",        :ruby_type => :boolean},
     91 => {:type => "DATE",           :ruby_type => :datetime},
     92 => {:type => "TIME",           :ruby_type => :timestamp},
     93 => {:type => "TIMESTAMP",      :ruby_type => :timestamp},
    100 => {:type => nil,              :ruby_type => :default},
     -1 => {:type => "LONG VARCHAR",   :ruby_type => :default},
     -2 => {:type => "BINARY",         :ruby_type => :default},
     -3 => {:type => "VARBINARY",      :ruby_type => :default},
     -4 => {:type => "LONG VARBINARY", :ruby_type => :default},
     -5 => {:type => "BIGINT",         :ruby_type => :integer},
     -6 => {:type => "TINYINT",        :ruby_type => :integer},
     -7 => {:type => "BIT",            :ruby_type => :default},
     -8 => {:type => "CHAR",           :ruby_type => :default},
    -10 => {:type => "BLOB",           :ruby_type => :default},
    -11 => {:type => "CLOB",           :ruby_type => :default},
  }

  class Database < RDBI::Database

    attr_accessor :handle

    def initialize(*args)
      super *args

      database = @connect_args[:database] || @connect_args[:dbname] ||
        @connect_args[:db]
      username = @connect_args[:username] || @connect_args[:user]
      password = @connect_args[:password] || @connect_args[:pass]

      @handle = ::ODBC.connect(database, username, password)

      self.database_name = @handle.get_info("SQL_DATABASE_NAME")
    end

    def disconnect
      @handle.rollback
      @handle.disconnect
      super
    end

    def transaction(&block)
      raise NotImplementedError, "#transaction"
    end

    def rollback
      @handle.rollback
      super
    end
      
    def commit
      @handle.commit
      super
    end

    def new_statement(query)
      Statement.new(query, self)
    end

    def table_schema(table_name)
      raise NotImplementedError, "#table_schema"
    end

    def schema
      raise NotImplementedError, "#schema"
    end

    def ping
      @handle.connected?
    end
  end

  class Cursor < RDBI::Cursor

    # only #fetch works reliably with ODBC, so we just build the array upfront.
    def initialize(handle)
      super handle
      @index = 0
      @rs = []
      while r = @handle.fetch
        @rs << r
      end
    end

    def next_row
      return nil if last_row?
      val = @rs[@index]
      @index += 1
      val
    end

    def result_count
      @rs.size
    end

    def affected_count
      0
    end

    def first
      @rs.first
    end

    def last
      @rs.last
    end

    def rest
      @rs[@index..-1]
    end

    def all
      @rs
    end

    def fetch(count = 1)
      return [] if last_row?
      @rs[@index, count]
    end

    def [](index)
      @rs[index]
    end

    def last_row?
      @index == @rs.size
    end

    def empty?
      @rs.empty?
    end

    def rewind
      @index = 0
    end

    def size
      @rs.length
    end

    def finish
      @handle.drop
    end

    def coerce_to_array
      @rs
    end
  end

  class Statement < RDBI::Statement

    attr_accessor :handle

    def initialize(query, dbh)
      super

      @handle = @dbh.handle.prepare(query)
      @output_type_map = RDBI::Type.create_type_hash(RDBI::Type::Out)
    end

    def new_execution(*binds)
      @handle.execute(*binds)

      columns = @handle.columns(true).collect do |col|
        newcol = RDBI::Column.new
        newcol.name        = col.name.to_sym
        newcol.type        = SQL_TYPES[col.type][:type]
        newcol.ruby_type   = SQL_TYPES[col.type][:ruby_type]
        newcol.precision   = col.precision
        newcol.scale       = col.scale
        newcol.nullable    = col.nullable
        newcol.table       = col.table
        newcol
      end

      return Cursor.new(@handle), RDBI::Schema.new(columns), @output_type_map
    end

    def finish
      @handle.drop
      super
    end
  end
end
