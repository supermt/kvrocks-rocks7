/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#pragma once

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cluster/cluster.h"
#include "cluster/migration_agent.h"
#include "cluster/replication.h"
#include "cluster/slot_import.h"
#include "cluster/slot_migrate.h"
#include "lua.hpp"
#include "rw_lock.h"
#include "stats/log_collector.h"
#include "stats/stats.h"
#include "storage/redis_metadata.h"
#include "storage/storage.h"
#include "task_runner.h"
#include "tls_util.h"
#include "worker.h"

struct DBScanInfo {
  time_t last_scan_time = 0;
  KeyNumStats key_num_stats;
  bool is_scanning = false;
};

struct ConnContext {
  Worker *owner;
  int fd;
  ConnContext(Worker *w, int fd) : owner(w), fd(fd) {}
};

struct StreamConsumer {
  Worker *owner;
  int fd;
  std::string ns;
  Redis::StreamEntryID last_consumed_id;
  StreamConsumer(Worker *w, int fd, std::string ns, Redis::StreamEntryID id)
      : owner(w), fd(fd), ns(std::move(ns)), last_consumed_id(id) {}
};

typedef struct {
  std::string channel;
  size_t subscribe_num;
} ChannelSubscribeNum;

enum SlowLog {
  kSlowLogMaxArgc = 32,
  kSlowLogMaxString = 128,
};

enum ClientType {
  kTypeNormal = (1ULL << 0),  // normal client
  kTypePubsub = (1ULL << 1),  // pubsub client
  kTypeMaster = (1ULL << 2),  // master client
  kTypeSlave = (1ULL << 3),   // slave client
};

enum ServerLogType { kServerLogNone, kReplIdLog };

class ServerLogData {
 public:
  // Redis::WriteBatchLogData always starts with digist ascii, we use alphabetic to
  // distinguish ServerLogData with Redis::WriteBatchLogData.
  static const char kReplIdTag = 'r';
  static bool IsServerLogData(const char *header) {
    if (header != NULL) return *header == kReplIdTag;
    return false;
  }

  ServerLogData() = default;
  explicit ServerLogData(ServerLogType type, const std::string &content) : type_(type), content_(content) {}

  ServerLogType GetType() { return type_; }
  std::string GetContent() { return content_; }
  std::string Encode();
  Status Decode(const rocksdb::Slice &blob);

 private:
  ServerLogType type_ = kServerLogNone;
  std::string content_;
};

class Server {
 public:
  explicit Server(Engine::Storage *storage, Config *config);
  ~Server();

  Status Start();
  void Stop();
  void Join();
  bool IsStopped() { return stop_; }
  bool IsLoading() { return is_loading_; }
  Config *GetConfig() { return config_; }
  Status LookupAndCreateCommand(const std::string &cmd_name, std::unique_ptr<Redis::Commander> *cmd);
  void AdjustOpenFilesLimit();

  Status AddMaster(std::string host, uint32_t port, bool force_reconnect);
  Status RemoveMaster();
  Status AddSlave(Redis::Connection *conn, rocksdb::SequenceNumber next_repl_seq);
  void DisconnectSlaves();
  void cleanupExitedSlaves();
  bool IsSlave() { return !master_host_.empty(); }
  void FeedMonitorConns(Redis::Connection *conn, const std::vector<std::string> &tokens);
  void IncrFetchFileThread() { fetch_file_threads_num_++; }
  void DecrFetchFileThread() { fetch_file_threads_num_--; }
  int GetFetchFileThreadNum() { return fetch_file_threads_num_; }

  int PublishMessage(const std::string &channel, const std::string &msg);
  void SubscribeChannel(const std::string &channel, Redis::Connection *conn);
  void UnSubscribeChannel(const std::string &channel, Redis::Connection *conn);
  void GetChannelsByPattern(const std::string &pattern, std::vector<std::string> *channels);
  void ListChannelSubscribeNum(std::vector<std::string> channels,
                               std::vector<ChannelSubscribeNum> *channel_subscribe_nums);
  void PSubscribeChannel(const std::string &pattern, Redis::Connection *conn);
  void PUnSubscribeChannel(const std::string &pattern, Redis::Connection *conn);
  int GetPubSubPatternSize() { return pubsub_patterns_.size(); }

  void AddBlockingKey(const std::string &key, Redis::Connection *conn);
  void UnBlockingKey(const std::string &key, Redis::Connection *conn);
  void BlockOnStreams(const std::vector<std::string> &keys, const std::vector<Redis::StreamEntryID> &entry_ids,
                      Redis::Connection *conn);
  void UnblockOnStreams(const std::vector<std::string> &keys, Redis::Connection *conn);
  Status WakeupBlockingConns(const std::string &key, size_t n_conns);
  Status OnEntryAddedToStream(const std::string &ns, const std::string &key, const Redis::StreamEntryID &entry_id);

  std::string GetLastRandomKeyCursor();
  void SetLastRandomKeyCursor(const std::string &cursor);

  static int GetUnixTime();
  void GetStatsInfo(std::string *info);
  void GetServerInfo(std::string *info);
  void GetMemoryInfo(std::string *info);
  void GetRocksDBInfo(std::string *info);
  void GetClientsInfo(std::string *info);
  void GetReplicationInfo(std::string *info);
  void GetRoleInfo(std::string *info);
  void GetCommandsStatsInfo(std::string *info);
  void GetInfo(const std::string &ns, const std::string &section, std::string *info);
  std::string GetRocksDBStatsJson();
  ReplState GetReplicationState();

  void PrepareRestoreDB();
  void WaitNoMigrateProcessing();
  Status AsyncCompactDB(const std::string &begin_key = "", const std::string &end_key = "");
  Status AsyncBgsaveDB();
  Status AsyncPurgeOldBackups(uint32_t num_backups_to_keep, uint32_t backup_max_keep_hours);
  Status AsyncScanDBSize(const std::string &ns);
  void GetLastestKeyNumStats(const std::string &ns, KeyNumStats *stats);
  time_t GetLastScanTime(const std::string &ns);

  int DecrClientNum();
  int IncrClientNum();
  int IncrMonitorClientNum();
  int DecrMonitorClientNum();
  int IncrBlockedClientNum();
  int DecrBlockedClientNum();
  std::string GetClientsStr();
  std::atomic<uint64_t> *GetClientID();
  void KillClient(int64_t *killed, std::string addr, uint64_t id, uint64_t type, bool skipme, Redis::Connection *conn);

  lua_State *Lua() { return lua_; }
  Status ScriptExists(const std::string &sha);
  Status ScriptGet(const std::string &sha, std::string *body);
  void ScriptSet(const std::string &sha, const std::string &body);
  void ScriptReset();
  void ScriptFlush();

  Status Propagate(const std::string &channel, const std::vector<std::string> &tokens);
  Status ExecPropagatedCommand(const std::vector<std::string> &tokens);
  Status ExecPropagateScriptCommand(const std::vector<std::string> &tokens);

  void SetCurrentConnection(Redis::Connection *conn) { curr_connection_ = conn; }
  Redis::Connection *GetCurrentConnection() { return curr_connection_; }

  LogCollector<PerfEntry> *GetPerfLog() { return &perf_log_; }
  LogCollector<SlowEntry> *GetSlowLog() { return &slow_log_; }
  void SlowlogPushEntryIfNeeded(const std::vector<std::string> *args, uint64_t duration);

  std::unique_ptr<RWLock::ReadLock> WorkConcurrencyGuard();
  std::unique_ptr<RWLock::WriteLock> WorkExclusivityGuard();

  Stats stats_;
  Engine::Storage *storage_;
  std::unique_ptr<Cluster> cluster_;
  static std::atomic<int> unix_time_;
  std::unique_ptr<class SlotMigrate> slot_migrate_;
  class SlotImport *slot_import_ = nullptr;
  std::unique_ptr<Engine::MigrationAgent> mg_agent;

#ifdef ENABLE_OPENSSL
  UniqueSSLContext ssl_ctx_;
#endif

 private:
  void cron();
  void recordInstantaneousMetrics();
  void delConnContext(ConnContext *c);
  void updateCachedTime();
  Status autoResizeBlockAndSST();

  std::atomic<bool> stop_;
  std::atomic<bool> is_loading_;
  time_t start_time_ = 0;
  std::mutex slaveof_mu_;
  std::string master_host_;
  uint32_t master_port_ = 0;
  Config *config_ = nullptr;
  std::string last_random_key_cursor_;
  std::mutex last_random_key_cursor_mu_;

  lua_State *lua_;

  Redis::Connection *curr_connection_ = nullptr;

  // client counters
  std::atomic<uint64_t> client_id_{1};
  std::atomic<int> connected_clients_{0};
  std::atomic<int> monitor_clients_{0};
  std::atomic<uint64_t> total_clients_{0};
  std::atomic<int> excuting_command_num_{0};

  // slave
  std::mutex slave_threads_mu_;
  std::list<FeedSlaveThread *> slave_threads_;
  std::atomic<int> fetch_file_threads_num_;

  // Some jobs to operate DB should be unique
  std::mutex db_job_mu_;
  bool db_compacting_ = false;
  bool is_bgsave_in_progress_ = false;
  int last_bgsave_time_ = -1;
  std::string last_bgsave_status_ = "ok";
  int last_bgsave_time_sec_ = -1;

  std::map<std::string, DBScanInfo> db_scan_infos_;

  LogCollector<SlowEntry> slow_log_;
  LogCollector<PerfEntry> perf_log_;

  std::map<ConnContext *, bool> conn_ctxs_;
  std::map<std::string, std::list<ConnContext *>> pubsub_channels_;
  std::map<std::string, std::list<ConnContext *>> pubsub_patterns_;
  std::mutex pubsub_channels_mu_;
  std::map<std::string, std::list<ConnContext *>> blocking_keys_;
  std::mutex blocking_keys_mu_;
  std::atomic<int> blocked_clients_{0};
  std::map<std::string, std::set<std::shared_ptr<StreamConsumer>>> blocked_stream_consumers_;

  // threads
  RWLock::ReadWriteLock works_concurrency_rw_lock_;
  std::thread cron_thread_;
  std::thread compaction_checker_thread_;
  TaskRunner task_runner_;
  std::vector<std::unique_ptr<WorkerThread>> worker_threads_;
  std::unique_ptr<ReplicationThread> replication_thread_;

  // memory
  int64_t memory_startup_use_ = 0;
};

Server *GetServer();
