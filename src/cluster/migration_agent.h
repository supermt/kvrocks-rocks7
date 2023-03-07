//
// Created by supermt on 2/28/23.
//

#ifndef KVROCKS_MIGRATION_AGENT_H
#define KVROCKS_MIGRATION_AGENT_H
#pragma once
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <rocksdb/table.h>

#include "config/config.h"
#include "server/server.h"
#include "storage/redis_db.h"

namespace Engine {

using rocksdb::Slice;
class MigrationAgent : public Redis::Database {
 public:
  explicit MigrationAgent(Config* config, Storage* storage, Server* svr);
  //  ~MigrationAgent() { std::cout << "stop agent" << std::endl; }

  Status ExecuteMigrationInBackground(std::string dst_ip, int dst_port, std::vector<int>& slots);
  Status publish_agent_command_multi();
  Status publish_agent_command(std::string dst_ip, int dst_port, int migrate_slot);

  struct SST_content {
    std::vector<std::pair<Slice, Slice>> meta_content;
    std::vector<std::pair<Slice, Slice>> subkey_content;
    uint64_t meta_size;
    uint64_t subkeydata_size;
    SST_content() : meta_content(0), subkey_content(0), meta_size(0), subkeydata_size(0) {}
    void clear() {
      meta_content.clear();
      subkey_content.clear();
    }
  };
  struct Ingestion_candidate {
    std::vector<std::string> meta_ssts;
    std::vector<std::string> subkey_ssts;
    Ingestion_candidate() : meta_ssts(0), subkey_ssts(0) {}
  };

 private:
  //  typedef std::unordered_map<std::string, std::vector<std::pair<Slice, Slice>>*> SST_content;
  //  typedef std::unordered_map<std::string, std::vector<std::string>> Ingestion_candidate;
  void call_to_seek_and_dump_agent(int migrate_slot, std::string namespace_, const rocksdb::Snapshot* slot_snapshot_,
                                   std::string dst_ip, int dst_port);
  Status ExtractOneRecord(const rocksdb::Slice& key, const Slice& metadata, SST_content* result_bucket,
                          const rocksdb::Snapshot* slot_snapshot_);
  bool ExtractSimpleRecord(const rocksdb::Slice& key, const Slice& metadata, SST_content* result_bucket);
  bool ExtractComplexRecord(const rocksdb::Slice& key, const Metadata& metadata, SST_content* result_bucket,
                            const rocksdb::Snapshot* migration_snapshot);
  Status DumpContentToSST(SST_content* result_bucket, Ingestion_candidate* sst_map, bool force);
  void create_thread(std::thread** migration_worker, int migration_slot);

  void call_to_seek_and_insert_agent();
  void call_to_level_agent();
  void call_to_batch_agent();
  void call_to_fusion_agent();

  bool SetDstImportStatus(int sock_fd, int status, int migration_slot);
  Status Finish(int migration_slot, int sock_fd);
  const uint64_t min_aggregation_size = 64 * 1024 * 1024l;
  Config* config_;
  Storage* storage_;
  Server* svr_;
  rocksdb::DB* db_ptr;
  int port_;
  std::string ip_;
  ParserState stat_ = ArrayLen;
  rocksdb::Env* env_;
  int sock_fd_;

  std::vector<int> slots_;
  std::string dst_host_;
  int dst_port_;
  void Fail(int slot, int fd);
  bool CheckResponseWithCounts(int sock_fd, int total);
};

}  // namespace Engine

#endif  // KVROCKS_MIGRATION_AGENT_H
