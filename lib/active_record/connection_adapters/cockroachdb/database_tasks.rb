# frozen_string_literal: true

require 'active_record/base'

module ActiveRecord
  module ConnectionAdapters
    module CockroachDB
      class DatabaseTasks < ActiveRecord::Tasks::PostgreSQLDatabaseTasks
        def structure_dump(_filename, _extra_flags = nil)
          raise 'db:structure:dump is unimplemented. See https://github.com/cockroachdb/activerecord-cockroachdb-adapter/issues/2'
        end

        def structure_load(_filename, _extra_flags = nil)
          raise 'db:structure:load is unimplemented. See https://github.com/cockroachdb/activerecord-cockroachdb-adapter/issues/2'
        end
      end
    end
  end
end

ActiveRecord::Tasks::DatabaseTasks.register_task(/cockroachdb/, ActiveRecord::ConnectionAdapters::CockroachDB::DatabaseTasks)
