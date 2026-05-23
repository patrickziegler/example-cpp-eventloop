#include <gtest/gtest.h>

#include <thread>
#include <queue>

namespace async {

using Clock = std::chrono::steady_clock;
using Delay = std::chrono::milliseconds;
using TimePont = Clock::time_point;
using Fn = std::function<void()>;

class TaskState {
public:
    void cancel() {
        m_cancelled = true;
    }
    bool isCancelled() const {
        return m_cancelled;
    }
private:
    bool m_cancelled = false;
};

using TaskHandle = std::shared_ptr<TaskState>;

struct Task {
    TaskHandle handle;
    std::function<void()> fn;
};

struct Timer : public Task {
    TimePont executeAt;
    bool operator>(const Timer& other) const {
        return executeAt > other.executeAt;
    }
};

class EventLoop {
public:
    auto post(Fn fn) -> TaskHandle {
        std::lock_guard<std::mutex> guard(m_mtx);
        auto h = std::make_shared<TaskState>();
        m_tasks.push(Task{h, std::move(fn)});
        m_cv.notify_one();
        return h;
    }
    auto post(Delay delay, Fn fn) -> TaskHandle {
        std::lock_guard<std::mutex> guard(m_mtx);
        auto h = std::make_shared<TaskState>();
        m_timers.push(Timer{h, std::move(fn), Clock::now() + delay});
        m_cv.notify_one();
        return h;
    }
    auto flush() {
        std::lock_guard<std::mutex> guard(m_mtx);
        while (!m_tasks.empty()) {
            m_tasks.pop();
        }
        while (!m_timers.empty()) {
            m_timers.pop();
        }
    }
    void run() {
        std::unique_lock<std::mutex> lock(m_mtx);
        if (m_running) {
            throw std::runtime_error("EventLoop is already running");
        }
        m_running = true;
        while (m_running) {
            if (!m_timers.empty() && m_timers.top().executeAt <= Clock::now()) {
                Task task = m_timers.top();
                m_timers.pop();
                m_tasks.push(std::move(task));
            } else if (!m_tasks.empty()) {
                auto task = m_tasks.front();
                m_tasks.pop();
                if (!task.handle->isCancelled()) {
                    lock.unlock();
                    try {
                        task.fn();
                    } catch (const std::exception& e) {
                        std::cerr << "Exception during task execution: " << e.what() << '\n';
                    } catch(...) {
                        std::cerr << "Unkown exception during task execution\n";
                    }
                    lock.lock();
                }
            } else if (!m_timers.empty()) {
                m_cv.wait_until(lock, m_timers.top().executeAt);
            } else {
                m_cv.wait(lock);
            }
        }
    }
    void stop() {
        m_running = false;
        m_cv.notify_all();
    }
private:
    std::condition_variable m_cv;
    std::mutex m_mtx;
    std::atomic_bool m_running = false;
    std::queue<Task> m_tasks;
    std::priority_queue<Timer, std::vector<Timer>, std::greater<>> m_timers;
};

} // namespace async

class TestEventLoop : public testing::Test {
public:
};

class Counter {
public:
    auto check(size_t val) {
        ASSERT_EQ(val, m_val);
        ++m_val;
    }
private:
    size_t m_val = 1;
};

TEST_F(TestEventLoop, PostTasks) {
    using namespace async;
    EventLoop loop;
    auto counter = std::make_shared<Counter>();
    loop.post([counter] {
        counter->check(1);
    });
    loop.post([counter] {
        counter->check(2);
    });
    loop.post([counter] {
        counter->check(3);
    });
    loop.post([&loop] {
        loop.stop();
    });
    loop.run();
    counter->check(4);
}

TEST_F(TestEventLoop, PostNestedTasks) {
    using namespace async;
    EventLoop loop;
    auto counter = std::make_shared<Counter>();
    loop.post([counter, &loop] {
        loop.post([counter, &loop] {
            counter->check(3);
            loop.stop();
        });
        counter->check(1);
    });
    loop.post([counter] {
        counter->check(2);
    });
    loop.run();
    counter->check(4);
}

TEST_F(TestEventLoop, PostDelayedTasks) {
    using namespace async;
    EventLoop loop;
    auto counter = std::make_shared<Counter>();
    loop.post(std::chrono::milliseconds(10), [counter] {
        counter->check(2);
    });
    loop.post(std::chrono::milliseconds(15), [counter, &loop] {
        counter->check(3);
        loop.stop();
    });
    loop.post([counter] {
        counter->check(1);
    });
    loop.run();
    counter->check(4);
}

TEST_F(TestEventLoop, ThrowExceptionTasks) {
    using namespace async;
    EventLoop loop;
    auto counter = std::make_shared<Counter>();
    loop.post([counter] {
        counter->check(1);
        throw std::runtime_error("just a test error, don't worry");
    });
    loop.post([counter, &loop] {
        counter->check(2);
        loop.stop();
    });
    loop.run();
    counter->check(3);
}

TEST_F(TestEventLoop, CancelTasks) {
    using namespace async;
    EventLoop loop;
    auto counter = std::make_shared<Counter>();
    auto h1 = loop.post([counter] {
        counter->check(999);
    });
    auto h2 = loop.post(std::chrono::milliseconds(10), [counter] {
        counter->check(999);
    });
    loop.post(std::chrono::milliseconds(15), [counter, &loop] {
        counter->check(1);
        loop.stop();
    });
    h1->cancel();
    h2->cancel();
    loop.run();
    counter->check(2);
}

TEST_F(TestEventLoop, StopSleepingEventLoop) {
    using namespace async;
    EventLoop loop;
    std::thread stopper([&loop] {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        loop.stop();
    });
    loop.run();
    stopper.join();
}

TEST_F(TestEventLoop, FlushQueues) {
    using namespace async;
    EventLoop loop;
    auto counter = std::make_shared<Counter>();
    loop.post([counter, &loop] {
        loop.flush();
        counter->check(1);
    });
    for (size_t i = 1; i <= 10; ++i) {
        loop.post([counter] {
            counter->check(999);
        });
    }
    std::thread stopper([&loop] {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        loop.stop();
    });
    loop.run();
    stopper.join();
    counter->check(2);
}

class ThreadedExecutor : public std::enable_shared_from_this<ThreadedExecutor> {
public:
    static auto create(std::shared_ptr<async::EventLoop> loop) {
        return std::shared_ptr<ThreadedExecutor>(new ThreadedExecutor(std::move(loop)));
    }
    void doSomethingLightAsync() {
        m_loop->post([] {
            std::cout << "good day!\n";
        });
    }
    void doSomethingHeavyAsync() {
        // ThreadedExecutor must be owned by std::shared_ptr for this
        auto weak = weak_from_this();
        m_loop->post([weak]() {
            std::thread([weak](){
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
                if (auto self = weak.lock()) {
                    self->doSomethingLightAsync();
                } else {
                    std::cout << "dang!\n";
                }
            }).detach(); // TODO: consider thread pool instead of detached thread
        });
    }
private:
    explicit ThreadedExecutor(std::shared_ptr<async::EventLoop> loop)
        : m_loop{std::move(loop)} {}
    std::shared_ptr<async::EventLoop> m_loop;
};

/*
TEST_F(TestEventLoop, Example1) {
    using namespace async;

    auto loop = std::make_shared<EventLoop>();

    std::thread producer([loop]() {
        for (int i = 1; i <= 5; ++i) {
            std::cout << "Sending event " << i << " from thread "
                      << std::this_thread::get_id()
                      << std::endl;
            loop->post([i]() {
                std::cout << "Event " << i
                          << " handled on thread "
                          << std::this_thread::get_id()
                          << std::endl;
            });
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        loop->post([&]() {
            std::cout << "Stopping loop" << std::endl;;
            loop->stop();
        });
    });

    std::cout << "Running loop on main thread "
              << std::this_thread::get_id()
              << std::endl;;

    loop->run();
    producer.join();
    std::cout << "Done" << std::endl;
}
*/
