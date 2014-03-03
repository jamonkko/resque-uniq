require 'test/unit'
require 'mocha/test_unit'
require 'resque'
require 'resque/plugins/unique_job'

class UniqueJobTest < Test::Unit::TestCase
  class Job
    extend Resque::Plugins::UniqueJob
    @queue = :job_test
    def self.perform(param) ; end
  end

  class AutoexpireLockJob
    extend Resque::Plugins::UniqueJob
    @queue = :job_test
    @unique_lock_autoexpire = 1
    def self.perform(param) ; end
  end

  class AutoexpireLockJobBase
    extend Resque::Plugins::UniqueJob
    def self.queue ; :job_test ; end
    def self.unique_lock_autoexpire ; 1 ; end
    def self.perform(param) ; end
  end

  class ExtendedAutoExpireLockJob < AutoexpireLockJobBase ; end

  def setup
    Job.unstub(:get_stale_lock)
    Resque.redis.flushdb
  end

  def test_no_more_than_one_job_instance
    queue = Resque.queue_from_class(Job)

    3.times { Resque.enqueue(Job, "hello") }
    assert_equal 1, Resque.size(queue)

    3.times { Resque.enqueue(Job, "bye") }
    assert_equal 2, Resque.size(queue)
  end

  def test_lock_is_removed_after_job_run
    queue = Resque.queue_from_class(Job)
    Resque.enqueue(Job, "hello")
    assert_equal 1, Resque.size(queue)

    worker = Resque::Worker.new(queue)
    job = worker.reserve
    worker.perform(job)
    assert_equal 0, Resque.size(queue)

    Resque.enqueue(Job, "hello")
    assert_equal 1, Resque.size(queue)
  end

  def test_lock_is_removed_after_dequeue
    queue = Resque.queue_from_class(Job)
    Resque.enqueue(Job, "hello")
    assert_equal 1, Resque.size(queue)

    Resque.dequeue(Job, "hello")
    assert_equal 0, Resque.size(queue)
    assert_equal nil, Resque.redis.get(Job.lock("hello"))
    assert_equal nil, Resque.redis.get(Job.run_lock("hello"))

    Resque.enqueue(Job, "hello")
    assert_equal 1, Resque.size(queue)
  end

  def test_stale_locks_are_cleared_if_no_worker_is_working
    queue = Resque.queue_from_class(Job)
    lock = Job.lock("hello")
    rlock = Job.run_lock("hello")
    Resque.redis.set(lock, Time.now.to_i)
    Resque.redis.set(rlock, Time.now.to_i)
    assert_equal 0, Resque.size(queue)

    Resque.enqueue(Job, "hello")
    assert_equal 1, Resque.size(queue)
  end

  def test_other_process_deletes_stale_lock_before_us
    queue = Resque.queue_from_class(Job)
    lock = Job.lock("hello")
    rlock = Job.run_lock("hello")
    stale_time = Time.now.to_i
    Resque.redis.set(lock, stale_time)
    Resque.redis.set(rlock, stale_time)

    # Fake other process deleting (and setting) lock after we detect stale lock, but before we delete it
    delete_locks = Proc.new do
      Resque.redis.set(lock, stale_time + 10)
    end
    Job.stubs(:get_stale_lock).with(&delete_locks).returns(stale_time)
    Resque.enqueue(Job, "hello")

    # Our job should not be enqueued, the fake job has the lock
    assert_equal 0, Resque.size(queue)
  end

  # XXX Resque doesn't call any job hooks in Resque#remove_queue. We don't get a chance to clean up the locks
  # def test_lock_is_removed_after_remove_queue
  #   queue = Resque.queue_from_class(Job)
  #   Resque.enqueue(Job, "hello")
  #   assert_equal 1, Resque.size(queue)

  #   Resque.remove_queue(queue)
  #   assert_equal nil, Resque.redis.get(Job.lock("hello"))
  #   assert_equal nil, Resque.redis.get(Job.run_lock("hello"))
  # end

  def test_autoexpire_lock
    Resque.enqueue(AutoexpireLockJob, 123)
    sleep 2
    Resque.enqueue(AutoexpireLockJob, 123)
    assert_equal 2, Resque.size(Resque.queue_from_class(AutoexpireLockJob))
  end

  def test_extended_autoexpire_lock
    Resque.enqueue(ExtendedAutoExpireLockJob, 123)
    sleep 2
    Resque.enqueue(ExtendedAutoExpireLockJob, 123)
    assert_equal 2, Resque.size(Resque.queue_from_class(ExtendedAutoExpireLockJob))
  end

  def test_cleans_up_old_lock_during_enqueue
    Resque.redis.set(AutoexpireLockJob.lock(123), Time.now.to_i - 100)
    Resque.enqueue(AutoexpireLockJob, 123)
    assert_equal 1, Resque.size(Resque.queue_from_class(AutoexpireLockJob))
  end
end
