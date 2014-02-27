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

      def get_stale_lock(lock)
        stale_value = get_lock(lock)
        return nil unless stale_value

        rlock = run_lock_from_lock(lock)
        return nil unless get_lock(rlock)

        Resque.working.map {|w| w.job }.map do |item|
          begin
            payload = item['payload']
            klass = Resque::Job.constantize(payload['class'])
            args = payload['args']
            return nil if rlock == klass.run_lock(*args)
          rescue NameError
            # unknown job class, ignore
          end
        end
        stale_value
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
          result = Resque.redis.eval(script, :keys => [lock], :argv => [Time.now.to_i - ttl]).to_i
          if (result == 0)
            nil
          else
            result
          end
        end
      end

      def before_enqueue_lock(*args)
        lock = lock(*args)

        stale_value = get_stale_lock(lock)
        if stale_value
          got_lock = reset_stale_lock(lock, stale_value)
        end

        got_lock ||= Resque.redis.setnx(lock, new_value(stale_value))
        if got_lock and ttl && ttl > 0
          Resque.redis.expire(lock, ttl)
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

      def reset_stale_lock(lock, stale_value)
        # Steal stale lock atomically to make sure that only the first process detecting stale lock will do it.
        # Only delete and get the lock if it still exists with the old stale value, otherwise someone else took it already.
        script = <<-LUA
          local current_value = redis.call('GET', KEYS[1]);
          if (tonumber(current_value) == tonumber(ARGV[2])) then
            redis.call('DEL', KEYS[2]);
            redis.call('SET', KEYS[1], ARGV[1]);
            return 1;
          else
            return 0;
          end
        LUA
        new_value = new_value(stale_value)
        result = Resque.redis.eval(script, :keys => [lock, run_lock_from_lock(lock)], :argv => [new_value, stale_value])
        result.to_i == 1
      end

      # return new value and make sure it is not the same as old value
      def new_value(old_value)
        val = Time.now.to_i
        if val == old_value
          val + 1
        else
          val
        end
      end

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
