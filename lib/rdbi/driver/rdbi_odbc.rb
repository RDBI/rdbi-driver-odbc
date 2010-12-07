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

    def initialize(handle)
      super handle
      @index = 0
    end

    def next_row
      val = if @array_handle
              @array_handle[@index]
            else
              @handle.fetch
            end
      @index += 1
      val
    end

    def result_count
      if @array_handle
        @array_handle.size
      else
        @handle.nrows
      end
    end

    def affected_count
      if @array_handle
        0
      else
        @handle.nrows
      end
    end

    def first
      if @array_handle
        @array_handle.first
      else
        @handle.fetch_first
      end
    end

    def last
      if @array_handle
        @array_handle.last
      else
        @handle.fetch_scroll(::ODBC::SQL_FETCH_LAST)
      end
    end

    def rest
      if @array_handle
        @array_handle[@index+1..-1]
      else
        @handle.fetch_all[1..-1]
      end
    end

    def all
      if @array_handle
        @array_handle
      else
        @handle.fetch_scroll(::ODBC::SQL_FETCH_FIRST) +
          @handle.fetch_all
      end
    end

    def fetch(count = 1)
      return [] if last_row?
      if @array_handle
        @array_handle[@index, count]
      else
        @handle.fetch_many(count)
      end
    end

    def [](index)
      if @array_handle
        @array_handle[index]
      else
        @handle.fetch_scroll(::ODBC::SQL_FETCH_ABSOLUTE, index)
      end
    end

    def last_row?
      if @array_handle
        @index == @array_handle.size
      else
        @index == @handle.nrows
      end
    end

    def empty?
      if @array_handle
        @array_handle.empty?
      else
        @handle.nrows == 0
      end
    end

    def rewind
      return if @index == 0
      @index = 0
      unless @array_handle
        @handle.fetch_first
      end
    end

    def size
      if @array_handle
        @array_handle.length
      else
        @handle.nrows
      end
    end

    def finish
      @handle.drop
    end

    def coerce_to_array
      unless @array_handle
        @array_handle = []
        begin
          @array_handle << @handle.fetch_first
          @array_handle += @handle.fetch_all
        rescue
        end
      end
    end
  end

  class Statement < RDBI::Statement

    attr_accessor :handle

    def initialize(query, dbh)
      super

      ep = Epoxy.new(query)
      @handle = @dbh.handle.prepare(ep.query)
      @output_type_map = RDBI::Type.create_type_hash(RDBI::Type::Out)
    end

    def new_execution(*binds)
      @handle.execute(*binds)

      columns = @handle.columns(true).collect do |col|
        newcol = RDBI::Column.new
        newcol.name = col.name.to_sym
        newcol
      end

      return Cursor.new(@handle), RDBI::Schema.new(columns), @output_type_map
    end

    def finish
      @handle.drop
    end
  end
end
