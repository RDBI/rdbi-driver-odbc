require 'rdbi'
require 'epoxy'
require 'methlab'

gem 'ruby-odbc', '= 0.99992'
require 'odbc'

class RDBI::Driver::ODBC < RDBI::Driver
  def initialize(*args)
    super Database, *args
  end
end

class RDBI::Driver::ODBC < RDBI::Driver
  class Database < RDBI::Database
    extend MethLab

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
      raise NotImplementedError, "not supported"
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
      raise NotImplementedError, "not supported"
    end

    def schema
      raise NotImplementedError, "not supported"
    end

    def ping
      @handle.connected?
    end
  end

  class Cursor < RDBI::Cursor
  end

  class Statement < RDBI::Statement
  end
end
