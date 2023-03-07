//
// Created by supermt on 2/28/23.
//

#include "migration_agent.h"

#include "event_util.h"

namespace Engine {
MigrationAgent::MigrationAgent(Config* config, Storage* storage, Server* svr)
    : Redis::Database(storage, rocksdb::kDefaultColumnFamilyName), svr_(svr), slots_(0), dst_host_(""), dst_port_(0) {
  port_ = config->migration_agent_port;
  ip_ = config->migration_agent_ip;

  LOG(INFO) << "[Migration Agent] @ " << ip_ << ":" << port_ << "creating" << std::endl;
  config_ = config;
  storage_ = storage;
  db_ = storage_->GetDB();
  env_ = db_->GetEnv();
  auto s = rocksdb::DB::OpenForReadOnly(rocksdb::Options(), storage->GetDB()->GetName(), &db_ptr);
  assert(s.ok());
}

inline void JoinThreads(std::vector<std::thread*>& thread_pool) {
  for (std::thread* thread : thread_pool) {
    thread->join();
  }
  return;
}

Status MigrationAgent::ExecuteMigrationInBackground(std::string dst_ip, int dst_port, std::vector<int>& slots) {
  std::thread* thread = nullptr;
  auto s = Util::SockConnect(dst_ip, dst_port, &sock_fd_);
  if (!s.IsOK()) {
    return s;
  }
  assert(sock_fd_ != -1);
  SetDstImportStatus(sock_fd_, kImportStart, kImportStart);

  thread = new std::thread(&MigrationAgent::publish_agent_command_multi, this);
  thread->detach();
  return Status::OK();
}
Status MigrationAgent::publish_agent_command_multi() {
  auto thread_pool_depth = config_->max_migration_compaction;
  std::vector<std::thread*> thread_pool;
  int count = 0;
  for (auto slot : slots_) {
    thread_pool.emplace_back();
    create_thread(&thread_pool[count], slot);
    if (count == thread_pool_depth) {
      JoinThreads(thread_pool);
      thread_pool.clear();
      count = 0;
    }
    count++;
  }
  JoinThreads(thread_pool);
  return Status::OK();
}

Status MigrationAgent::publish_agent_command(std::string dst_ip, int dst_port, int migrate_slot) {
  // add the migration controller if needed
  std::thread* migration_worker = nullptr;
  create_thread(&migration_worker, migrate_slot);
  migration_worker->detach();

  return Status::OK();
}
void MigrationAgent::call_to_seek_and_dump_agent(int migrate_slot, std::string namespace_,
                                                 const rocksdb::Snapshot* slot_snapshot_, std::string dst_ip,
                                                 int dst_port) {
  int slot = migrate_slot;
  rocksdb::ReadOptions read_options;
  read_options.snapshot = slot_snapshot_;
  read_options.fill_cache = false;
  rocksdb::ColumnFamilyHandle* cf_handle = storage_->GetCFHandle(Engine::kMetadataColumnFamilyName);
  std::unique_ptr<rocksdb::Iterator> iter(storage_->GetDB()->NewIterator(read_options, cf_handle));
  bool stop_migrate_ = false;
  std::string prefix;
  ComposeSlotKeyPrefix(namespace_, slot, &prefix);
  LOG(INFO) << "[Agent] Iterate keys of slot, key's prefix: " << prefix;
  LOG(INFO) << "[Agent] Start migrating snapshot of slot " << slot;
  iter->Seek(prefix);

  Slice current_migrate_key;

  SST_content temp_result;
  Ingestion_candidate migration_ssts;
  auto start = Util::GetTimeStampMS();
  // Seek to the beginning of keys start with 'prefix' and iterate all these keys
  for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
    // The migrating task has to be stopped, if server role is changed from master to slave
    // or flush command (flushdb or flushall) is executed
    if (stop_migrate_) {
      LOG(ERROR) << "[migrate] Stop migrating snapshot due to the thread stopped";
      return;
    }

    // Iteration is out of range
    if (!iter->key().starts_with(prefix)) {
      break;
    }

    // Get user key
    std::string ns, user_key;
    ExtractNamespaceKey(iter->key(), &ns, &user_key, true);
    // extract tables for ingestion.
    ExtractOneRecord(user_key, iter->value(), &temp_result, slot_snapshot_);
    DumpContentToSST(&temp_result, &migration_ssts, false);
  }

  // flush all entries to SST
  DumpContentToSST(&temp_result, &migration_ssts, true);

  auto end = Util::GetTimeStampMS();

  LOG(INFO) << "[Agent] Iterate and SST dump finished, time (ms): " << end - start;

  temp_result.clear();
  int target_fd = -1;
  auto s = Util::SockConnect(dst_ip, dst_port, &target_fd);

  std::string command_head = "";
  command_head = command_head + "ingest " + kMetadataColumnFamilyName + " true ";
  for (auto file : migration_ssts.meta_ssts) {
    std::string cmd = command_head + file;
    s = Util::SockSend(sock_fd_, cmd);
    if (!s.IsOK()) {
      Fail(migrate_slot, sock_fd_);
    }
  }
  command_head.clear();
  command_head = command_head + "ingest " + kSubkeyColumnFamilyName + " true ";
  for (auto file : migration_ssts.subkey_ssts) {
    std::string cmd = command_head + file;
    s = Util::SockSend(sock_fd_, cmd);
  }

  LOG(INFO) << "[Agent] Succeed to migrate slot snapshot, slot: " << slot;
  Finish(migrate_slot, sock_fd_);
}

Status MigrationAgent::ExtractOneRecord(const rocksdb::Slice& key, const Slice& meta_value, SST_content* result_bucket,
                                        const rocksdb::Snapshot* snapshot) {
  std::string prefix_key;
  AppendNamespacePrefix(key, &prefix_key);
  std::string bytes = meta_value.ToString();
  Metadata metadata(kRedisNone, false);
  metadata.Decode(bytes);

  if (metadata.Type() != kRedisString && metadata.size == 0) {
    return Status(Status::cOK, "empty");
  }
  if (metadata.Expired()) {
    return Status(Status::cOK, "expired");
  }

  switch (metadata.Type()) {
    case kRedisString: {
      bool s = ExtractSimpleRecord(key, meta_value, result_bucket);
      if (!s) {
        return Status::NotOK;
      }
      break;
    }
    case kRedisList:
    case kRedisZSet:
    case kRedisBitmap:
    case kRedisHash:
    case kRedisSet:
    case kRedisSortedint: {
      bool s = ExtractComplexRecord(key, metadata, result_bucket, snapshot);
      if (!s) {
        return Status::NotOK;
      }

      break;
    }
    default:
      break;
  }

  return Status::OK();
}
bool MigrationAgent::ExtractSimpleRecord(const rocksdb::Slice& key, const Slice& value, SST_content* result_bucket) {
  auto list = result_bucket->meta_content;
  result_bucket->meta_size += (key.size() + value.size());
  list.emplace_back(key, value);
  return true;
}

bool MigrationAgent::ExtractComplexRecord(const rocksdb::Slice& key, const Metadata& metadata,
                                          SST_content* result_bucket, const rocksdb::Snapshot* migration_snapshot) {
  rocksdb::ReadOptions read_opt;
  read_opt.snapshot = migration_snapshot;
  read_opt.fill_cache = false;  // we can make some changes here
  rocksdb::ColumnFamilyHandle* sub_key_handle = storage_->GetCFHandle(kSubkeyColumnFamilyName);
  std::unique_ptr<rocksdb::Iterator> iter(db_ptr->NewIterator(read_opt, sub_key_handle));

  std::string slot_key, prefix_subkey;

  AppendNamespacePrefix(key, &slot_key);

  InternalKey(slot_key, "", metadata.version, true).Encode(&prefix_subkey);

  //  int item_count = 0;
  for (iter->Seek(prefix_subkey); iter->Valid(); iter->Next()) {
    if (!iter->key().starts_with(prefix_subkey)) {
      break;
    }
    // the iter key here is the key stored in ssts
    result_bucket->subkey_content.emplace_back(iter->key(), iter->value());
    result_bucket->subkeydata_size += (iter->key().size(), iter->value().size());
  }
  return true;
}

Status MigrationAgent::DumpContentToSST(SST_content* result_bucket, Ingestion_candidate* sst_map, bool force) {
  //  db_ptr->
  auto meta_cf = storage_->GetCFHandle(kMetadataColumnFamilyName);
  auto subkey_cf = storage_->GetCFHandle(kSubkeyColumnFamilyName);

  std::string meta_sst = "";
  std::string subkey_sst = "";
  std::string migration_dir = "/tmp/migration/";
  env_->CreateDirIfMissing(migration_dir);
  meta_sst = migration_dir + "meta" + std::to_string(Util::GetTimeStampMS()) + ".sst";
  subkey_sst = migration_dir + "subkey" + std::to_string(Util::GetTimeStampMS()) + ".sst";

  if (force) {
    db_ptr->CreateTableBuilder(meta_cf->GetID(), meta_cf, -1, result_bucket->meta_content, meta_sst);
    sst_map->meta_ssts.push_back(meta_sst);
    db_ptr->CreateTableBuilder(meta_cf->GetID(), subkey_cf, -1, result_bucket->meta_content, subkey_sst);
    sst_map->subkey_ssts.push_back(subkey_sst);
  } else {
    // pick only the content array that is filled
    if (result_bucket->meta_size > min_aggregation_size) {
      LOG(INFO) << "meta column full, generate SST";
      db_ptr->CreateTableBuilder(meta_cf->GetID(), meta_cf, -1, result_bucket->meta_content, meta_sst);
      sst_map->meta_ssts.push_back(meta_sst);
    }
    if (result_bucket->subkeydata_size > min_aggregation_size) {
      LOG(INFO) << "subkey column full, generate SST";
      db_ptr->CreateTableBuilder(meta_cf->GetID(), subkey_cf, -1, result_bucket->meta_content, subkey_sst);
      sst_map->subkey_ssts.push_back(subkey_sst);
    }
  }

  return Status::NotOK;
}

void MigrationAgent::call_to_level_agent() { std::cout << "Not supported yep" << std::endl; }
void MigrationAgent::call_to_batch_agent() { std::cout << "Not supported yep" << std::endl; }
void MigrationAgent::call_to_iteration_agent() { std::cout << "Not supported yep" << std::endl; }
void MigrationAgent::call_to_fusion_agent() { std::cout << "Not supported yep" << std::endl; }

void MigrationAgent::create_thread(std::thread** migration_worker, int migrate_slot) {
  auto mode = MigrationMode(config_->batch_migrate);
  switch (mode) {
    case kSeekAndInsert:
      *migration_worker = new std::thread(&MigrationAgent::call_to_iteration_agent, this);
      return;
    case kSeekAndDump:
      *migration_worker = new std::thread(&MigrationAgent::call_to_seek_and_dump_agent, this, migrate_slot, namespace_,
                                          this->storage_->GetDB()->GetSnapshot(), dst_host_, dst_port_);
      return;
    case kCompactAndMerge:
      *migration_worker = new std::thread(&MigrationAgent::call_to_batch_agent, this);
      return;
    case kLevelMigration:
      *migration_worker = new std::thread(&MigrationAgent::call_to_level_agent, this);
      return;
    case kFusion:
      *migration_worker = new std::thread(&MigrationAgent::call_to_fusion_agent, this);
      return;
  }
}
Status MigrationAgent::Finish(int migrate_slot, int sock_fd) {
  //  if (sock_fd <= 0) {
  //    LOG(ERROR) << "[agent] Empty socket";
  //    return Status::NotOK;
  //  }
  bool import_success = SetDstImportStatus(sock_fd, kImportSuccess, migrate_slot);
  if (!import_success) {
    return Status(Status::NotOK, "Failed to notify import");
  }
  std::string dst_ip_port = dst_host_ + ":" + std::to_string(dst_port_);
  Status st = svr_->cluster_->SetSlotMigrated(migrate_slot, dst_ip_port);

  return st;
}
bool MigrationAgent::SetDstImportStatus(int sock_fd, int status, int migration_slot) {
  if (sock_fd <= 0) return false;

  std::string cmd =
      Redis::MultiBulkString({"cluster", "import", std::to_string(migration_slot), std::to_string(status)});
  auto s = Util::SockSend(sock_fd, cmd);
  if (!s.IsOK()) {
    LOG(ERROR) << "[migrate] Failed to send import command to destination, slot: " << migration_slot
               << ", error: " << s.Msg();
    return false;
  }

  return CheckResponseWithCounts(sock_fd, 1);
}
void MigrationAgent::Fail(int slot, int fd) { SetDstImportStatus(sock_fd_, kImportFailed, slot); }

bool MigrationAgent::CheckResponseWithCounts(int sock_fd, int total) {
  if (sock_fd < 0 || total <= 0) {
    LOG(INFO) << "[migrate] Invalid args, sock_fd: " << sock_fd << ", count: " << total;
    return false;
  }

  // Set socket receive timeout first
  struct timeval tv;
  tv.tv_sec = 1;
  tv.tv_usec = 0;
  setsockopt(sock_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

  // Start checking response
  size_t bulk_len = 0;
  int cnt = 0;
  stat_ = ArrayLen;
  UniqueEvbuf evbuf;
  while (true) {
    // Read response data from socket buffer to the event buffer
    if (evbuffer_read(evbuf.get(), sock_fd, -1) <= 0) {
      LOG(ERROR) << "[migrate] Failed to read response, Err: " + std::string(strerror(errno));
      return false;
    }

    // Parse response data in event buffer
    bool run = true;
    while (run) {
      switch (stat_) {
        // Handle single string response
        case ArrayLen: {
          UniqueEvbufReadln line(evbuf.get(), EVBUFFER_EOL_CRLF_STRICT);
          if (!line) {
            LOG(INFO) << "[migrate] Event buffer is empty, read socket again";
            run = false;
            break;
          }

          if (line[0] == '-') {
            LOG(ERROR) << "[migrate] Got invalid response: " + std::string(line.get())
                       << ", line length: " << line.length;
            stat_ = Error;
          } else if (line[0] == '$') {
            auto parse_result = ParseInt<uint64_t>(std::string(line.get() + 1, line.length - 1), 10);
            if (!parse_result) {
              LOG(ERROR) << "[migrate] Protocol Err: expect integer";
              stat_ = Error;
            } else {
              bulk_len = *parse_result;
              stat_ = bulk_len > 0 ? BulkData : OneRspEnd;
            }
          } else if (line[0] == '+' || line[0] == ':') {
            stat_ = OneRspEnd;
          } else {
            LOG(ERROR) << "[migrate] Unexpected response: " << line.get();
            stat_ = Error;
          }

          break;
        }
        // Handle bulk string response
        case BulkData: {
          if (evbuffer_get_length(evbuf.get()) < bulk_len + 2) {
            LOG(INFO) << "[migrate] Bulk data in event buffer is not complete, read socket again";
            run = false;
            break;
          }
          // TODO(chrisZMF): Check tail '\r\n'
          evbuffer_drain(evbuf.get(), bulk_len + 2);
          bulk_len = 0;
          stat_ = OneRspEnd;
          break;
        }
        case OneRspEnd: {
          cnt++;
          if (cnt >= total) {
            return true;
          }
          stat_ = ArrayLen;
          break;
        }
        case Error: {
          return false;
        }
        default:
          break;
      }
    }
  }
}
}  // namespace Engine