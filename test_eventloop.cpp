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

struct TimedTask : public Task {
    TimePont executeAt;
    bool operator>(const TimedTask& other) const {
        return executeAt > other.executeAt;
    }
};

class SimpleEventLoop {
public:
    auto post(Fn fn) -> TaskHandle {
        auto h = std::make_shared<TaskState>();
        m_tasks.push(Task{h, std::move(fn)});
        return h;
    }
    auto postDelayed(Delay delay, Fn fn) -> TaskHandle {
        auto h = std::make_shared<TaskState>();
        m_timedTasks.push(TimedTask{h, std::move(fn), Clock::now() + delay});
        return h;
    }
    void run() {
        while (!m_tasks.empty() || !m_timedTasks.empty()) {
            if (!m_timedTasks.empty() && m_timedTasks.top().executeAt <= Clock::now()) {
                Task task = m_timedTasks.top();
                m_timedTasks.pop();
                m_tasks.push(std::move(task));
            } else if (!m_tasks.empty()) {
                auto task = m_tasks.front();
                m_tasks.pop();
                if (!task.handle->isCancelled()) {
                    try {
                        task.fn();
                    }  catch (const std::exception& e) {
                        std::cerr << "Exception during task execution: " << e.what() << '\n';
                    } catch(...) {
                        std::cerr << "Unkown exception during task execution\n";
                    }
                }
            } else {
                std::this_thread::sleep_until(m_timedTasks.top().executeAt);
            }
        }
    }
private:
    std::queue<Task> m_tasks;
    std::priority_queue<TimedTask, std::vector<TimedTask>, std::greater<>> m_timedTasks;
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
    auto postDelayed(Delay delay, Fn fn) -> TaskHandle {
        std::lock_guard<std::mutex> guard(m_mtx);
        auto h = std::make_shared<TaskState>();
        m_timedTasks.push(TimedTask{h, std::move(fn), Clock::now() + delay});
        m_cv.notify_one();
        return h;
    }
    void run() {
        std::unique_lock<std::mutex> lock(m_mtx);
        if (m_running) {
            throw std::runtime_error("EventLoop is already running");
        }
        m_running = true;
        while (m_running) {
            if (!m_timedTasks.empty() && m_timedTasks.top().executeAt <= Clock::now()) {
                Task task = m_timedTasks.top();
                m_timedTasks.pop();
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
            } else if (!m_timedTasks.empty()) {
                m_cv.wait_until(lock, m_timedTasks.top().executeAt);
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
    std::mutex m_mtx;
    std::condition_variable m_cv;
    std::atomic_bool m_running = false;
    std::queue<Task> m_tasks;
    std::priority_queue<TimedTask, std::vector<TimedTask>, std::greater<>> m_timedTasks;
};

} // namespace async

class TestSimpleEventLoop : public testing::Test {
public:
    void check(size_t val) {
        ASSERT_EQ(val, m_val);
        ++m_val;
    }
private:
    size_t m_val = 1;
};

TEST_F(TestSimpleEventLoop, Example1) {
    using namespace async;

    SimpleEventLoop loop;

    loop.post([this, &loop]() {
        check(1);
        loop.post([this]() {
            check(3);
        });
    });

    loop.post([this]() {
        check(2);
    });

    auto h1 = loop.post([this]() {
        check(999);
    });
    h1->cancel();

    loop.postDelayed(Delay{5}, [this]() {
        check(4);
    });

    loop.run();
}

class TestEventLoop : public testing::Test {
public:
};

TEST_F(TestEventLoop, StopSleepingEventLoop) {
    using namespace async;

    EventLoop loop;

    std::thread stopper([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        loop.stop();
    });

    loop.post([] {
        std::cout << "started event loop\n";
    });
    loop.run();

    stopper.join();
    std::cout << "stopped event loop\n";
}

class Foo : public std::enable_shared_from_this<Foo> {
public:
    explicit Foo(std::shared_ptr<async::EventLoop> loop)
        : m_loop{std::move(loop)} {}
    void doSomethingLightAsync() {
        m_loop->post([] {
            std::cout << "good day!\n";
        });
    }
    void doSomethingHeavyAsync() {
        // Foo must be owned by std::shared_ptr for this
        auto weak = weak_from_this();
        m_loop->post([weak]() {
            std::thread([weak](){
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
                if (auto self = weak.lock()) {
                    self->doSomethingLightAsync();
                } else {
                    std::cout << "dang!\n";
                }
            }).detach();
        });
    }
private:
    std::shared_ptr<async::EventLoop> m_loop;
};

TEST_F(TestEventLoop, ExecuteInThread) {
    using namespace async;

    auto loop = std::make_shared<EventLoop>();
    auto foo = std::make_shared<Foo>(loop);
    foo->doSomethingHeavyAsync();

    std::thread stopper([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        loop->stop();
    });

    loop->post([] {
        std::cout << "started event loop\n";
    });
    loop->run();

    stopper.join();
    std::cout << "stopped event loop\n";
}

TEST_F(TestEventLoop, Example1) {
    using namespace async;

    EventLoop loop;

    std::thread producer([&loop]() {
        for (int i = 1; i <= 5; ++i) {
            std::cout << "Sending event " << i << " from thread "
                      << std::this_thread::get_id()
                      << std::endl;
            loop.post([i]() {
                std::cout << "Event " << i
                          << " handled on thread "
                          << std::this_thread::get_id()
                          << std::endl;
            });
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        loop.post([&]() {
            std::cout << "Stopping loop" << std::endl;;
            loop.stop();
        });
    });

    std::cout << "Running loop on main thread "
              << std::this_thread::get_id()
              << std::endl;;

    loop.run();
    producer.join();
    std::cout << "Done" << std::endl;
}
