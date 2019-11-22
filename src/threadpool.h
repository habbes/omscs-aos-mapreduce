#ifndef __threadpool_h__
#define __threadpool_h__

#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>

class threadpool {
public:
    threadpool(const unsigned int num_threads);
    ~threadpool();
    void enqueue_task(std::function<void(void)>);
    void shutdown();
    void thread_loop(int);
    
private:
    std::vector<std::thread *> threads_;
    std::queue<std::function<void(void)>> tasks_;
    std::mutex lock_;
    std::condition_variable cond_have_tasks_;
    int num_threads_;
    bool shutting_down_;
};

#endif

