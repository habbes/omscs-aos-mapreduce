#include <thread>
#include "threadpool.h"

threadpool::threadpool(const unsigned int num_threads): num_threads_(num_threads)
{
    threads_.reserve(num_threads_);
    shutting_down_ = false;
    for (int i = 0; i < num_threads; i++) {
        threads_.push_back(new std::thread(std::bind(&threadpool::thread_loop, this, i)));
    }
}

threadpool::~threadpool()
{
    shutdown();
}

void threadpool::shutdown()
{
    {
        std::unique_lock<std::mutex> l(lock_);
        shutting_down_ = true;
        cond_have_tasks_.notify_all();
    }
    for (auto t : threads_) {
        t->join();
        delete t;
    }
}

void threadpool::thread_loop(int i)
{
    printf("Thread %d started\n", i);
    std::function<void(void)> task;
    while (1) {
        {
            std::unique_lock<std::mutex> l(lock_);
            while (!shutting_down_ && tasks_.empty()) {
                cond_have_tasks_.wait(l);
            }
            if (tasks_.empty()) {
                // shutting down
                printf("Thread %d is shutting down\n", i);
                return;
            }

            task = std::move(tasks_.front());
            tasks_.pop();
        }
        printf("Thread %d is executing a task\n", i);
        task();
    }
}

void threadpool::enqueue_task(std::function<void(void)> fn)
{
    std::unique_lock<std::mutex> l(lock_);
    tasks_.emplace(std::move(fn));
    cond_have_tasks_.notify_one();
}
