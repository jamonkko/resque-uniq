module Resque
  module Plugins
    module UniqueJob
      LOCK_NAME_PREFIX = 'lock'
      RUN_LOCK_NAME_PREFIX = 'running_'

      def lock(*args)
        "#{LOCK_NAME_PREFIX}:#{name}-#{obj_to_string(args)}"
      end

      def run_lock(*args)
        run_lock_from_lock(lock(*args))
      end

      def run_lock_from_lock(lock)
        "#{RUN_LOCK_NAME_PREFIX}#{lock}"
      end

      def lock_from_run_lock(rlock)
        rlock.sub(/^#{RUN_LOCK_NAME_PREFIX}/, '')
      end

      def stale_lock?(lock)
        return false unless get_lock(lock)

        rlock = run_lock_from_lock(lock)
        return false unless get_lock(rlock)

        Resque::WorkerRegistry.working.map {|w| w.worker_registry.job }.map do |item|
          begin
            payload = item['payload']
            klass = payload['class'].to_s.constantize
            args = payload['args']
            return false if rlock == klass.run_lock(*args)
          rescue NameError
            # unknown job class, ignore
          end
        end
        true
      end

      def ttl
        instance_variable_get(:@unique_lock_autoexpire) || respond_to?(:unique_lock_autoexpire) && unique_lock_autoexpire
      end

      def get_lock(lock)
        lock_value = Resque.backend.store.get(lock)
        set_time = lock_value.to_i
        if ttl && lock_value && (set_time < Time.now.to_i - ttl)
          Resque.backend.store.del(lock)
          nil
        else
          lock_value
        end
      end

      def before_enqueue_lock(*args)
        lock_name = lock(*args)
        if stale_lock? lock_name
          Resque.backend.store.del lock_name
          Resque.backend.store.del run_lock_from_lock(lock_name)
        end
        not_exist = Resque.backend.store.setnx(lock_name, Time.now.to_i)
        if not_exist
          if ttl && ttl > 0
            Resque.backend.store.expire(lock_name, ttl)
          end
        end
        not_exist
      end

      def around_perform_lock(*args)
        # we must calculate the lock name before executing job's perform method, it can modify *args
        jlock = lock(*args)

        rlock = run_lock(*args)
        Resque.backend.store.set(rlock, Time.now.to_i)

        begin
          yield
        ensure
          Resque.backend.store.del(rlock)
          Resque.backend.store.del(jlock)
        end
      end

      def after_dequeue_lock(*args)
        Resque.backend.store.del(run_lock(*args))
        Resque.backend.store.del(lock(*args))
      end

      private

      def obj_to_string(obj)
        case obj
        when Hash
          s = []
          obj.keys.sort.each do |k|
            s << obj_to_string(k)
            s << obj_to_string(obj[k])
          end
          s.to_s
        when Array
          s = []
          obj.each { |a| s << obj_to_string(a) }
          s.to_s
        else
          obj.to_s
        end
      end
    end
  end
end
