# frozen_string_literal: true

require 'active_record/connection_adapters/abstract/transaction'

module ActiveRecord
  module ConnectionAdapters
    # NOTE(joey): This is a very sad monkey patch. Unfortunately, it is
    # required in order to prevent doing more than 2 nested transactions
    # while still allowing a single nested transaction. This is because
    # CockroachDB only supports a single savepoint at the beginning of a
    # transaction. Allowing this works for the common case of testing.
    module CockroachDB
      module TransactionManagerMonkeyPatch
        def begin_transaction(options = {})
          @connection.lock.synchronize do
            # If the transaction nesting is already 2 deep, raise an error.
            if @connection.adapter_name == 'CockroachDB' && @stack.is_a?(ActiveRecord::ConnectionAdapters::SavepointTransaction)
              raise(ArgumentError, 'cannot nest more than 1 transaction at a time. this is a CockroachDB limitation')
            end
          end
          super(options)
        end

        def within_new_transaction(options = {})
          attempts = options.fetch(:attempts, 0)
          super
        rescue ActiveRecord::SerializationFailure => error
          raise if attempts >= @connection.max_transaction_retries

          attempts += 1
          sleep_seconds = (2 ** attempts + rand) / 10
          sleep(sleep_seconds)
          within_new_transaction(options.merge(attempts: attempts)) { yield }
        end
      end
    end

    class TransactionManager
      prepend CockroachDB::TransactionManagerMonkeyPatch
    end
  end
end
