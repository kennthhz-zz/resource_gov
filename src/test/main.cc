#include <functional>
#include <vector>
#include <thread>         
#include <chrono>   
#include <map>
#include <set>
#include <sched.h>
#include "task_queue.h"
#include <limits.h>
#include <sched.h>
#include <atomic>
#include <math.h> 
#include <uuid/uuid.h>
#include <stdexcept>

void TestDefaultPartitionPlacementStrategy() {
  const int partitionSize = IResourceGov::total_vcores * 
    (IResourceGov::total_reserved_vcore / IResourceGov::reserved_vcore_per_partition);
  int vcore_start_index = 8;
  std::map<int, int> vcore_count;

  DefaultPartitionPlacementStrategy s;
  s.Init(vcore_start_index);

  for (int i = 0; i < IResourceGov::total_vcores; i++) {
    vcore_count[vcore_start_index + i] = 0;
  }

  // init partitions
  auto partitions = std::vector<std::string>();
  for (int i = 0; i < partitionSize; i++) {
    uuid_t id;
    char uuid_string[37];
    uuid_generate(id);
    uuid_unparse(id, uuid_string);
    partitions.emplace_back(uuid_string, 36);
  }

  s.AddPartition(partitions);
  for (auto partition : partitions) {
    auto vcore = s.GetPartitionVcore(partition);
    if (vcore_count.find(vcore) == vcore_count.end()) {
      // failure: found a vcore that is not in range.
      throw std::logic_error("vcore is out of range");
    }

    vcore_count[vcore]++;
  }

  // each vcore should have IResourceGov::total_reserved_vcore / IResourceGov::reserved_vcore_per_partition 
  const int partitionsPerVcore =
    IResourceGov::total_reserved_vcore / IResourceGov::reserved_vcore_per_partition;
  for (auto [key, count] : vcore_count) {
    if (count != partitionsPerVcore) {
      // failure: 
      throw std::logic_error("each vcore's partition count incorrect");
    }
  }

  auto count = 0;
  std::map<std::string, std::string> after_remove;
  for (auto partition : partitions) {
    count++;
    if (count > IResourceGov::total_vcores) {
      after_remove.emplace(partition, partition);
    } else {
      s.RemovePartition(partition);
    }
  }

  vcore_count.clear();
  for (int i = 0; i < IResourceGov::total_vcores; i++) {
    vcore_count[vcore_start_index + i] = 0;
  }

  count = 0;
  auto pair = s.GetPartitionToVCoreIterator();
  for (auto it = pair.first; it != pair.second; it++) {
    if (after_remove.find(it->first) == after_remove.end()) {
      throw std::logic_error("find unexpected partition after remove");
    }

    auto vcore = s.GetPartitionVcore(it->first);
    if (vcore_count.find(vcore) == vcore_count.end()) {
      // failure: found a vcore that is not in range.
      throw std::logic_error("vcore is out of range");
    }

    vcore_count[vcore]++;
    count++;
  }

  if (count != partitionSize - IResourceGov::total_vcores) {
    throw std::logic_error("count incorrect after remove");
  }

  for (auto [key, val] : vcore_count) {
    if (val != (partitionSize - IResourceGov::total_vcores)/ IResourceGov::total_vcores) {
       throw std::logic_error("per vcore partition count incorrect after remove");
    }
  }

  std::cout<<"TestDefaultPartitionPlacementStrategy succeeded\n";
}

void TestTasks() {
  const int partitionSize = IResourceGov::total_vcores * 2;
  int vcore_start_index = 8;

  // init partitions
  auto partitions = std::vector<std::string>();
  for (int i = 0; i < partitionSize; i++) {
    uuid_t id;
    char uuid_string[37];
    uuid_generate(id);
    uuid_unparse(id, uuid_string);
    partitions.emplace_back(uuid_string, 36);
  }

  auto resourceGov = std::make_shared<ResourceGov>(vcore_start_index, 
    std::move(std::make_unique<DefaultPartitionPlacementStrategy>()));

  resourceGov->AddPartition(partitions, std::make_shared<PartitionResConfig>());

  TaskQueue taskq(resourceGov);
  srand (time(NULL));
  std::atomic<int> totalCompletedTasks = 0;
  const int totalTasks = 2000000;

  for (int i = 0; i < totalTasks; i++) {
    int index = rand() % partitionSize;
    int index2 = rand() % 2;
    auto partId = partitions[index];

    if (index2 == 0) { 
      taskq.EnqueueComputeTask(partId, [&totalCompletedTasks] {  
        for (int a = 0; a < 10000; a++) {
        }
        totalCompletedTasks++;
        });
    } else {
      taskq.EnqueueIOTask(partId, [&totalCompletedTasks] {  
        for (int a = 0; a < 100; a++) {
        }
        std::this_thread::sleep_for (std::chrono::microseconds(1));
        totalCompletedTasks++;
        });
    }
  }

  while (totalCompletedTasks != totalTasks) {
    std::this_thread::sleep_for (std::chrono::seconds(1));
  }

  taskq.Shutdown();
  std::cout<<"TestTasks succeeded\n";
}

int main() {
  TestDefaultPartitionPlacementStrategy();
  TestTasks();
}
