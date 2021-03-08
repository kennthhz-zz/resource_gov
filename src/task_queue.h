#pragma once
#include <functional>
#include <vector>
#include <thread>         
#include <chrono>   
#include <map>
#include <set>
#include <sched.h>
#include "MpScQueue.h"
#include <limits.h>
#include <sched.h>
#include <atomic>
#include <math.h> 
#include <uuid/uuid.h>
#include <stdexcept>
#include "resource_gov.h"

class TaskQueue final {
 public:
  TaskQueue(std::shared_ptr<IResourceGov> config) : 
    config_{config} {
    for (int i = 0; i < IResourceGov::total_vcores; i++) {
      isComputeShuttingDown[i] = false;
      compute_task_queues[i] = new MpScQueue<std::function<void(void)>>(10000);
      compute_threads_[i] = new std::thread([this, i]{ this->ComputeTaskWrapper(i);});

      // affinitize thread to vcore 
      cpu_set_t cpuset;
      CPU_ZERO(&cpuset);
      CPU_SET(i + config_->get_vcore_start_index(), &cpuset);
      int rc = pthread_setaffinity_np(compute_threads_[i]->native_handle(), sizeof(cpu_set_t), &cpuset);
      if (rc != 0) {
      }

      
      for (int j = 0; j < IResourceGov::io_threads_per_core; j++) {
        isIOShuttingDown[i * IResourceGov::io_threads_per_core + j] = false;
        io_task_queues[i * IResourceGov::io_threads_per_core + j] = new MpScQueue<std::function<void(void)>>(20000);
        io_threads_[i * IResourceGov::io_threads_per_core + j] = 
          new std::thread([this, i, j]{ this->IOTaskWrapper(i * IResourceGov::io_threads_per_core + j);});
      }

      for (int j = 0; j < IResourceGov::io_threads_per_core; j++) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i + config_->get_vcore_start_index(), &cpuset);
        int rc = pthread_setaffinity_np(io_threads_[i * IResourceGov::io_threads_per_core + j]->native_handle(), sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
        }
      }
    }
  }

  ~TaskQueue() {
    if (compute_task_queues != nullptr) {
      for (int i = 0; i < IResourceGov::total_vcores; i++) {
        delete compute_task_queues[i];
      }

      delete compute_task_queues;
    }
  }
  
  template<typename T, typename... Args>
  void EnqueueComputeTask(const std::string& partitionId, T&& computeTask, Args&&... args) {
    auto work = [computeTask,args...]() { computeTask(args...); };
    auto index = config_->GetPartitionVcore(partitionId) - config_->get_vcore_start_index();
    compute_task_queues[index]->enqueue(work); 
  } 

  template<typename T, typename... Args>
  void EnqueueIOTask(const std::string& partitionId, T&& ioTask, Args&&... args) {
    auto work = [ioTask, args...]() { ioTask(args...); };
    auto index = config_->GetPartitionVcore(partitionId) - config_->get_vcore_start_index();
    io_task_queues[index]->enqueue(work); 
  } 

  void Shutdown() {
    std::cout<<"Call to shutdown\n"
;    for (int i = 0; i < IResourceGov::total_vcores; i++) {
      isComputeShuttingDown[i] = true;
      compute_threads_[i]->join();

      for (int j = 0; j < IResourceGov::io_threads_per_core; j++) {
        isIOShuttingDown[i * IResourceGov::io_threads_per_core + j] = true;
        io_threads_[i * IResourceGov::io_threads_per_core + j]->join();
      }
    }
  }

 private:
  void ComputeTaskWrapper(int cpu_index) {
    while(true) {
      if (isComputeShuttingDown[cpu_index]) {
        std::cout<<"shutdown compute thread "<<cpu_index<<"\n";
        return;
      }

      std::function<void(void)> task;
      if (compute_task_queues[cpu_index]->dequeue(task)) {
        std::invoke(task);
      }
      else {
        std::this_thread::sleep_for (std::chrono::microseconds(5));
      }
    }
  }

  void IOTaskWrapper(int index) {
    while(true) {
      if (isIOShuttingDown[index]) {
        std::cout<<"shutdown IO thread "<<index<<"\n";
        return;
      }

      std::function<void(void)> task;
      if (io_task_queues[index]->dequeue(task)) {
        std::invoke(task);
      }
      else {
        std::this_thread::sleep_for (std::chrono::milliseconds(10));
      }
    }
  }

  MpScQueue<std::function<void(void)>>** compute_task_queues = 
    new MpScQueue<std::function<void(void)>>*[IResourceGov::total_vcores];
  std::thread** compute_threads_ = new std::thread*[IResourceGov::total_vcores];
  bool isComputeShuttingDown[IResourceGov::total_vcores];
  
  MpScQueue<std::function<void(void)>>** io_task_queues = 
    new MpScQueue<std::function<void(void)>>*[IResourceGov::total_vcores * IResourceGov::io_threads_per_core];
  std::thread** io_threads_ = new std::thread*[IResourceGov::total_vcores * IResourceGov::io_threads_per_core];

  bool isIOShuttingDown[IResourceGov::total_vcores * IResourceGov::io_threads_per_core];
  std::shared_ptr<IResourceGov> config_;
};