// biggie_record_store.cpp

/**
 *    Copyright (C) 2018 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage
#include <utility>
#include <cstring>

#include "mongo/db/storage/biggie/biggie_record_store.h"

// #include "mongo/db/jsobj.h"
// #include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/storage/biggie/biggie_store.h"
#include "mongo/db/storage/biggie/store.h"
// #include "mongo/db/storage/oplog_hack.h"
// #include "mongo/db/storage/recovery_unit.h"
// #include "mongo/stdx/memory.h"
// #include "mongo/util/log.h"
// #include "mongo/util/mongoutils/str.h"
// #include "mongo/util/unowned_ptr.h"

namespace mongo {

std::unique_ptr<BiggieStore> store;

const char* BiggieRecordStore::name() const {
    return "Biggie";
}

RecordData BiggieRecordStore::dataFor(OperationContext* opCtx, const RecordId& loc) const {
    RecordData rd;
    findRecord(opCtx, loc, &rd);
    return rd;  // should use std::move?
}

bool BiggieRecordStore::findRecord(OperationContext* opCtx, const RecordId& loc, RecordData* rd) const {
    // TODO: RecordId -> std::pair<u8*,size_t>
    // can't do this without a find
    // Key key(&(loc.repr()), 8);
    return false;
}
void BiggieRecordStore::deleteRecord(OperationContext* opCtx, const RecordId&) {
    // TODO: need to iterate through our store and delete the record
    return;
}

StatusWith<RecordId> BiggieRecordStore::insertRecord(
    OperationContext* opCtx, const char* data, int len, Timestamp, bool enforceQuota) {
    size_t num_chunks = 64 / sizeof(uint8_t);
    uint8_t* key_ptr = (uint8_t *) std::malloc(num_chunks);
    uint64_t thisRecordId = ++nextRecordId;
    std::memcpy(key_ptr, &thisRecordId, num_chunks);

    Key key(key_ptr, num_chunks);
    Value v(key, std::string(data, len));
    store->insert(v);
 
    RecordId rID(thisRecordId);
    return StatusWith<RecordId>(rID);
}
}  // namespace mongo
