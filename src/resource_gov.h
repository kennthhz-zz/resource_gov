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

// Summary: Store the config data for partition's
// resource governance rule
struct PartitionResConfig final {
  // If burstable is allowed for the partition
  // Bursting is allowed only for a short period of time
  // and is not guaranteed. when bursting, the partition
  // will use spare_vcore first, then use buffer_cpu.
  // If Burst_allowed is set to true, Use_spare_vcore_allowed must also be true.
  // If Use_spare_vcore_allowed is true, Burst_allowed doesn't have to be true.
  bool Burst_allowed;

  // If a vcore is not fully provisioned, allow this partition to use its share of spare vcore
  // For example, if a vcore only has 2 partitions provisioned, and buffer_vcore is 0.25, then
  // each parition has addition spare_vocre = (1 - 0.25 - (2 * 0.25))/2 = 0.125.
  // When the vcore is fully provisioned with 3 partitions, spare vcore would be 0.
  // Unlike bursting, spare_vcore can be used as long as there is spare on the CPU.
  bool Use_spare_vcore_allowed;
};

// Summary: Interface that defines Resource Governor instance's config
// Resourece Governor is one per POD. It can't span different NUMA node.
// It maps to a subset of vcores in a NUMA node.
class IResourceGov {
 public:
  // total vcores in a POD.
  static const int total_vcores = 8;

  // Reserved vcore fraction (<1) for per partition.
  static constexpr double reserved_vcore_per_partition = 0.25;

  // Maximum total reserved vcore fraction for all partitions in a vcore
  // For exmaple, if reserved_vcore_per_partition = 0.25 and we allow maximum 3 partitions
  // in a vcore, then total_reserved_vcore = 3 * 0.25 = 0.75.
  // Then buffer_vcore = 1-0.75 = 0.25
  static constexpr double total_reserved_vcore = 0.75;

  // Total vcores reserved for burstable + kernel + other overhead for a POD.
  static constexpr double buffer_vcore = 0.25;

  // Percentage of total_reserved_vcore is allocated for CPU used in IO tasks 
  // shared by all partitions. 
  // This number should small (such as 0.1) because our design is based on 
  // separating compute and I/O tasks. 
  // For example, if io_cpu_percentage = 0.1 and total_reserved_vcore = 0.75
  // Then total vcores in a POD allocated for cpu usage inside I/O task is
  // 20 * 0.75 * 0.1 = 1.5 vcore.
  static constexpr double io_cpu_percentage = 0.1;

  // 
  static const int io_threads_per_core = 3;

  // Starting vcore index of this POD
  // All vcores in a POD must be continuous 
  virtual const int get_vcore_start_index() = 0;

  virtual void AddPartition(const std::vector<std::string>& partitionIds, std::shared_ptr<PartitionResConfig> config) = 0;

  virtual void RemovePartition(const std::string& partitionId) = 0;

  virtual std::shared_ptr<PartitionResConfig> GetPartition(const std::string& partitionId) = 0;

  virtual int GetPartitionVcore(const std::string& partitionId) = 0;
};

class IPartitionPlacementStrategy {
 public:
    virtual void Init(int vcore_start_index) = 0;

    virtual void AddPartition(const std::vector<std::string>& partitionIds) = 0;

    virtual void RemovePartition(const std::string& partitionId) = 0;

    virtual int GetPartitionVcore(const std::string& partitionId) = 0;
};    

class DefaultPartitionPlacementStrategy final : public IPartitionPlacementStrategy {
 public:
  ~DefaultPartitionPlacementStrategy() { 
    if (partitionToVcore_ != nullptr)
      delete partitionToVcore_;
    
    if (vcoreToPartition_ != nullptr)
      delete vcoreToPartition_;
  }

  void Init(
    int vcore_start_index) override {
    partitionToVcore_ = new std::map<std::string, int>();
    vcoreToPartition_ = new std::map<int, std::set<std::string>>();
    for (int i = 0; i < IResourceGov::total_vcores; i++) {
      vcoreToPartition_.load()->emplace(vcore_start_index + i, std::set<std::string>());
    }
  }

  void AddPartition(const std::vector<std::string>& partitionIds) override {
    int low_size = INT_MAX;
    int low_vcore = -1;
    auto tmp_partitionToVcore = new std::map<std::string, int>(*partitionToVcore_);
    auto tmp_vcoreToPartition = new std::map<int, std::set<std::string>>(*vcoreToPartition_);

    for (auto const& partitionId : partitionIds) {
      for (auto const& [key, val] : (*tmp_vcoreToPartition)) {
        if (val.size() < low_size) {
          low_size = val.size();
          low_vcore = key;
        }
      }

      (*tmp_vcoreToPartition)[low_vcore].insert(partitionId);
      (*tmp_partitionToVcore)[partitionId] = low_vcore;
      low_size = INT_MAX;
      low_vcore = -1;
    }

    auto toDelete1 = partitionToVcore_.load();
    partitionToVcore_ = tmp_partitionToVcore;
    delete toDelete1;

    auto toDelete2 = vcoreToPartition_.load();
    vcoreToPartition_ = tmp_vcoreToPartition;
    delete toDelete2;
  }

  int GetPartitionVcore(const std::string& partitionId) override {
    return (*partitionToVcore_)[partitionId];
  }

  void RemovePartition(const std::string& partitionId) override {
    auto tmp_partitionToVcore = new std::map<std::string, int>(*partitionToVcore_);
    auto tmp_vcoreToPartition = new std::map<int, std::set<std::string>>(*vcoreToPartition_);

    int vcore = (*tmp_partitionToVcore)[partitionId];
    tmp_partitionToVcore->erase(partitionId);
    (*tmp_vcoreToPartition)[vcore].erase(partitionId);

    auto toDelete1 = partitionToVcore_.load();
    partitionToVcore_ = tmp_partitionToVcore;
    delete toDelete1;

    auto toDelete2 = vcoreToPartition_.load();
    vcoreToPartition_ = tmp_vcoreToPartition;
    delete toDelete2;
  }

  auto GetPartitionToVCoreIterator() {
    return std::pair(partitionToVcore_.load()->begin(), partitionToVcore_.load()->end());
  }


 private:
  std::atomic<std::map<std::string, int>*> partitionToVcore_;
  std::atomic<std::map<int, std::set<std::string>>*> vcoreToPartition_;
};

class ResourceGov final : public IResourceGov {
  public:
    ResourceGov(int vcore_start_index, std::unique_ptr<IPartitionPlacementStrategy> strategy) : 
      vcore_start_index_{vcore_start_index},
      strategy_{std::move(strategy)} {
        partitionToConfig_ = new std::map<std::string, std::shared_ptr<PartitionResConfig>>();
        strategy_->Init(vcore_start_index);
    }

    const int get_vcore_start_index() override {
      return vcore_start_index_;
    }

    std::shared_ptr<PartitionResConfig> GetPartition(const std::string& partitionId) override {
      return (*partitionToConfig_)[partitionId];
    }

    void AddPartition(const std::vector<std::string>& partitionIds, std::shared_ptr<PartitionResConfig> config) override {
      for (auto& partitionId : partitionIds) {
        (*partitionToConfig_)[partitionId] = config;
      }

      strategy_->AddPartition(partitionIds);
    }

    void RemovePartition(const std::string& partitionId) override {
      partitionToConfig_.load()->erase(partitionId);
      strategy_->RemovePartition(partitionId);
    }

    int GetPartitionVcore(const std::string& partitionId) override {
      return strategy_->GetPartitionVcore(partitionId);
    }

  private:
    int vcore_start_index_;
    std::atomic<std::map<std::string, std::shared_ptr<PartitionResConfig>>*> partitionToConfig_;
    std::unique_ptr<IPartitionPlacementStrategy> strategy_;
};