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

        Resque.working.map {|w| w.job }.map do |item|
          begin
            payload = item['payload']
            klass = Resque::Job.constantize(payload['class'])
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
        if not ttl
          Resque.redis.get(lock)
        else
          # Manually clean expired locks atomically to make sure value has not changed between get and delete
          script = <<-LUA
            local set_time = tonumber(redis.call('GET', KEYS[1]));
            if (set_time and set_time < tonumber(ARGV[1])) then
              redis.call('DEL', KEYS[1]);
              return 0;
            else
              return set_time;
            end
          LUA
          result = Resque.redis.eval(script, :keys => [lock], :argv => [Time.now.to_i - ttl])
          return nil if (result == 0)
          result
        end
      end

      def before_enqueue_lock(*args)
        lock_name = lock(*args)
        got_lock = false
        if stale_lock? lock_name
          # Steal stale lock atomically to make sure that only the first process detecting stale lock will do it.
          script = <<-LUA
            local lock_value = redis.call('GET', KEYS[1]);
            if (lock_value) then
              redis.call('DEL', KEYS[2]);
              redis.call('SET', KEYS[1], ARGV[1]);
              return 1;
            else
              return 0;
            end
          LUA
          result = Resque.redis.eval(script, :keys => [lock, rlock], :argv => [Time.now.to_i])
          got_lock = (result == 1)
        else
          got_lock = Resque.redis.setnx(lock_name, Time.now.to_i)
        end
        if got_lock and ttl && ttl > 0
          Resque.redis.expire(lock_name, ttl)
        end
        got_lock
      end

      def around_perform_lock(*args)
        # we must calculate the lock name before executing job's perform method, it can modify *args
        jlock = lock(*args)

        rlock = run_lock(*args)
        Resque.redis.set(rlock, Time.now.to_i)

        begin
          yield
        ensure
          Resque.redis.del(rlock)
          Resque.redis.del(jlock)
        end
      end

      def after_dequeue_lock(*args)
        Resque.redis.del(run_lock(*args))
        Resque.redis.del(lock(*args))
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
