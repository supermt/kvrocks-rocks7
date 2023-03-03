//
// Created by supermt on 2/28/23.
//

#include "migration_agent.h"

namespace Engine {
MigrationAgent::MigrationAgent(Config* config, Storage* storage)
    : Redis::Database(storage, rocksdb::kDefaultColumnFamilyName) {
  port_ = config->migration_agent_port;
  ip_ = config->migration_agent_ip;

  LOG(INFO) << "[Migration Agent] @ " << ip_ << ":" << port_ << "creating" << std::endl;
  config_ = config;
  storage_ = storage;
  auto s = rocksdb::DB::OpenForReadOnly(rocksdb::Options(), storage->GetDB()->GetName(), &db_ptr);
  assert(s.ok());
}
Status MigrationAgent::publish_agent_command(std::string dst_ip, int dst_port, int migrate_slot, std::string namespace_,
                                             const rocksdb::Snapshot* slot_snapshot_) {
  // add the migration controller if needed
  std::thread* migration_worker = nullptr;

  switch (config_->batch_migrate) {
    case 0:
      migration_worker = new std::thread(&MigrationAgent::call_to_iterate_agent, this, migrate_slot, namespace_,
                                         slot_snapshot_, dst_ip, dst_port);
      break;
    case 1:
      migration_worker = new std::thread(&MigrationAgent::call_to_batch_agent, this);
      break;
    case 2:
      migration_worker = new std::thread(&MigrationAgent::call_to_level_agent, this);
      break;
    default:
      migration_worker = new std::thread(&MigrationAgent::call_to_level_agent, this);
      break;
  }

  migration_worker->detach();

  return Status::OK();
}
void MigrationAgent::call_to_iterate_agent(int migrate_slot, std::string namespace_,
                                           const rocksdb::Snapshot* slot_snapshot_, std::string dst_ip, int dst_port) {
  int16_t slot = migrate_slot;
  rocksdb::ReadOptions read_options;
  read_options.snapshot = slot_snapshot_;
  read_options.fill_cache = false;
  rocksdb::ColumnFamilyHandle* cf_handle = storage_->GetCFHandle(Engine::kMetadataColumnFamilyName);
  std::unique_ptr<rocksdb::Iterator> iter(storage_->GetDB()->NewIterator(read_options, cf_handle));
  bool stop_migrate_ = false;
  std::string prefix;
  ComposeSlotKeyPrefix(namespace_, slot, &prefix);
  LOG(INFO) << "[migrate] Iterate keys of slot, key's prefix: " << prefix;
  LOG(INFO) << "[migrate] Start migrating snapshot of slot " << slot;
  iter->Seek(prefix);

  std::cout << "data" << std::endl;

  Slice current_migrate_key;

  SST_content temp_result;
  Ingestion_candidate migration_ssts;

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

  temp_result.clear();
  int target_fd = -1;
  auto s = Util::SockConnect(dst_ip, dst_port, &target_fd);

  std::string command_head = "";
  command_head = command_head + "ingest " + kMetadataColumnFamilyName + " true ";
  for (auto file : migration_ssts.meta_ssts) {
    std::string cmd = command_head + file;
    s = Util::SockSend(target_fd, cmd);
  }
  command_head.clear();
  command_head = command_head + "ingest " + kSubkeyColumnFamilyName + " true ";
  for (auto file : migration_ssts.subkey_ssts) {
    std::string cmd = command_head + file;
    s = Util::SockSend(target_fd, cmd);
  }

  LOG(INFO) << "[migrate] Succeed to migrate slot snapshot, slot: " << slot;
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
  auto migration_dir = storage_->GetMigrationTemp();
  env_->CreateDirIfMissing(migration_dir);

  if (force) {
    db_ptr->CreateTableBuilder(meta_cf->GetID(), meta_cf, -1, result_bucket->meta_content, meta_sst);
    sst_map->meta_ssts.push_back(meta_sst);
    db_ptr->CreateTableBuilder(meta_cf->GetID(), subkey_cf, -1, result_bucket->meta_content, subkey_sst);
    sst_map->subkey_ssts.push_back(subkey_sst);
  } else {
    // pick only the content array that is filled
    if (result_bucket->meta_size > min_aggregation_size) {
      db_ptr->CreateTableBuilder(meta_cf->GetID(), meta_cf, -1, result_bucket->meta_content, meta_sst);
      sst_map->meta_ssts.push_back(meta_sst);
    }
    if (result_bucket->subkeydata_size > min_aggregation_size) {
      db_ptr->CreateTableBuilder(meta_cf->GetID(), subkey_cf, -1, result_bucket->meta_content, subkey_sst);
      sst_map->subkey_ssts.push_back(subkey_sst);
    }
  }
  return Status::NotOK;
}

void MigrationAgent::call_to_level_agent() { std::cout << "Not supported yep" << std::endl; }
void MigrationAgent::call_to_batch_agent() { std::cout << "Not supported yep" << std::endl; }
}  // namespace Engine