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


// ALERT: need to remodify db.cpp to actually create an fcv on line about 422.
// (!storageGlobalParams.readOnly && (storageGlobalParams.engine != "devnull");)
// once this stuff actually gets implemented!!!

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/db/storage/biggie/biggie_record_store.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/storage/biggie/biggie_recovery_unit.h"
#include "mongo/db/storage/biggie/store.h"
#include "mongo/db/storage/key_string.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/hex.h"
#include "mongo/util/log.h"

#include <cstring>
#include <iomanip>
#include <memory>
#include <sstream>
#include <utility>

namespace mongo {
namespace biggie {
namespace {
Ordering allAscending = Ordering::make(BSONObj());
auto const version = KeyString::Version::V1;
BSONObj const sample = BSON(""
                            << "s"
                            << ""
                            << (int64_t)0);

std::string createKey(StringData ident, int64_t recordId) {
    KeyString ks(version, BSON("" << ident << "" << recordId), allAscending);
    return std::string(ks.getBuffer(), ks.getSize());
}
int64_t extractRecordId(const std::string& keyStr) {
    KeyString ks(version, sample, allAscending);
    ks.resetFromBuffer(keyStr.c_str(), keyStr.size());
    BSONObj obj = KeyString::toBson(keyStr.c_str(), keyStr.size(), allAscending, ks.getTypeBits());
    auto it = BSONObjIterator(obj);
    ++it;
    return (*it).Long();
}
StringStore* getRecoveryUnitBranch_forking(OperationContext* opCtx) {
    RecoveryUnit* biggieRCU = checked_cast<RecoveryUnit*>(opCtx->recoveryUnit());
    invariant(biggieRCU);
    biggieRCU->forkIfNeeded();
    return biggieRCU->getWorkingCopy();
}
}  // namespace

RecordStore::RecordStore(StringData ns,
                         StringData ident,
                         bool isCapped,
                         int64_t cappedMaxSize,
                         int64_t cappedMaxDocs,
                         CappedCallback* cappedCallback)
    : mongo::RecordStore(ns),
      _isCapped(isCapped),
      _cappedMaxSize(cappedMaxSize),
      _cappedMaxDocs(cappedMaxDocs),
      _identStr(ident.rawData(), ident.size()),
      _ident(_identStr.data(), _identStr.size()),
      _prefix(createKey(_ident, std::numeric_limits<int64_t>::min())),
      _postfix(createKey(_ident, std::numeric_limits<int64_t>::max())),
      _cappedCallback(cappedCallback) {
    log() << "RS created with " << _identStr;
}

const char* RecordStore::name() const {
    return "biggie";
}

const std::string& RecordStore::getIdent() const {
    return _identStr;
}

long long RecordStore::dataSize(OperationContext* opCtx) const {
    const StringStore* str = getRecoveryUnitBranch_forking(opCtx);
    size_t totalSize = 0;
    StringStore::const_iterator it = str->lower_bound(_prefix);
    StringStore::const_iterator end = str->upper_bound(_postfix);
    while (it != end) {
        totalSize += it->second.length();
        ++it;
    }
    return totalSize;
}


long long RecordStore::numRecords(OperationContext* opCtx) const {
    StringStore* str = getRecoveryUnitBranch_forking(opCtx);
    auto nR = str->distance(str->lower_bound(_prefix), str->upper_bound(_postfix));
    log() << "NR found: " << nR << " in " << _ident << " between "
          << toHex(_prefix.data(), _prefix.size()) << " "
          << toHex(_postfix.data(), _postfix.size());
    return nR;
}

bool RecordStore::isCapped() const {
    return _isCapped;
}

int64_t RecordStore::storageSize(OperationContext* opCtx,
                                 BSONObjBuilder* extraInfo,
                                 int infoLevel) const {
    return dataSize(opCtx);
}

RecordData RecordStore::dataFor(OperationContext* opCtx, const RecordId& loc) const {
    RecordData rd;
    invariant(findRecord(opCtx, loc, &rd));
    return rd;
}

bool RecordStore::findRecord(OperationContext* opCtx, const RecordId& loc, RecordData* rd) const {
    log() << "findR in ident " << _ident;
    const StringStore* workingCopy = getRecoveryUnitBranch_forking(opCtx);
    auto it = workingCopy->find(createKey(_ident, loc.repr()));
    if (it == workingCopy->end()) {
        return false;
    }
    *rd = RecordData(it->second.c_str(), it->second.length()).getOwned();
    return true;
}

void RecordStore::deleteRecord(OperationContext* opCtx, const RecordId& dl) {
    log() << "Delete called in " << _ident;
    StringStore* workingCopy = getRecoveryUnitBranch_forking(opCtx);
    auto numElementsRemoved = workingCopy->erase(createKey(_ident, dl.repr()));
    invariant(numElementsRemoved == 1);
}

StatusWith<RecordId> RecordStore::insertRecord(OperationContext* opCtx,
                                               const char* data,
                                               int len,
                                               Timestamp) {
    int64_t thisRecordId = nextRecordId();
    StringStore* workingCopy = getRecoveryUnitBranch_forking(opCtx);
    std::string key = createKey(_ident, thisRecordId);
    log() << "Inserting rec into " << _ident << " with key " << toHex(key.c_str(), key.length());
    auto rec = workingCopy->insert(StringStore::value_type{key, std::string(data, len)});
    if (!rec.second)
        log() << "Could not insert";
    return StatusWith<RecordId>(RecordId(thisRecordId));
}

Status RecordStore::insertRecordsWithDocWriter(OperationContext* opCtx,
                                               const DocWriter* const* docs,
                                               const Timestamp*,
                                               size_t nDocs,
                                               RecordId* idsOut) {
    // TODO : make this an actual optimization
    log() << "Docwriter";
    StringStore* workingCopy = getRecoveryUnitBranch_forking(opCtx);
    for (size_t i = 0; i < nDocs; i++) {
        int64_t thisRecordId = nextRecordId();
        std::string key = createKey(_ident, thisRecordId);
        const size_t len = docs[i]->documentSize();
        StringStore::value_type vt{key, std::string(len, '\0')};
        // TODO: change to .data() in c++17 once that is in the codebase
        docs[i]->writeDocument(&vt.second[0]);
        workingCopy->insert(std::move(vt));
        idsOut[i] = RecordId(thisRecordId);
    }
    return Status::OK();
}

Status RecordStore::updateRecord(OperationContext* opCtx,
                                 const RecordId& oldLocation,
                                 const char* data,
                                 int len,
                                 UpdateNotifier* notifier) {
    log() << "update in " << _ident;
    StringStore* workingCopy = getRecoveryUnitBranch_forking(opCtx);
    std::string key = createKey(_ident, oldLocation.repr());
    StringStore::iterator it = workingCopy->find(key);
    invariant(it != workingCopy->end());
    it->second.assign(data, len);
    return Status::OK();
}

bool RecordStore::updateWithDamagesSupported() const {
    return true;
}

StatusWith<RecordData> RecordStore::updateWithDamages(OperationContext* opCtx,
                                                      const RecordId& loc,
                                                      const RecordData& oldRec,
                                                      const char* damageSource,
                                                      const mutablebson::DamageVector& damages) {
    log() << "Updating with dams";
    StringStore* workingCopy = getRecoveryUnitBranch_forking(opCtx);
    std::string key = createKey(_ident, loc.repr());
    StringStore::iterator doc = workingCopy->find(key);
    invariant(doc != workingCopy->end());  // Only update existing records.
    for (const auto& d : damages) {
        const char* source = damageSource + d.sourceOffset;
        char* target = (&doc->second[0]) + d.targetOffset;
        std::memcpy(target, source, d.size);
    }
    RecordData updatedRecord(doc->second.c_str(), doc->second.length());
    return updatedRecord.getOwned();  // Data is un-owned.
}

std::unique_ptr<SeekableRecordCursor> RecordStore::getCursor(OperationContext* opCtx,
                                                             bool forward) const {
    if (forward)
        return std::make_unique<Cursor>(opCtx, *this);
    return std::make_unique<ReverseCursor>(opCtx, *this);
}

Status RecordStore::truncate(OperationContext* opCtx) {
    log() << "truncating " << _ident;
    StringStore* str = getRecoveryUnitBranch_forking(opCtx);
    StringStore::iterator it = str->lower_bound(_prefix);
    StringStore::iterator end = str->upper_bound(_postfix);
    std::vector<std::string> keysToErase;
    int64_t toErase = 0;
    while (it != end) {
        keysToErase.push_back(it->first);
        ++it;
        toErase++;
    }
    for (auto k : keysToErase) {
        toErase -= str->erase(k);
    }
    invariant(toErase == 0);
    return Status::OK();
}

void RecordStore::cappedTruncateAfter(OperationContext* opCtx, RecordId end, bool inclusive) {
    // TODO : implement.
}

Status RecordStore::validate(OperationContext* opCtx,
                             ValidateCmdLevel level,
                             ValidateAdaptor* adaptor,
                             ValidateResults* results,
                             BSONObjBuilder* output) {
    /* std::cout << "validate" << std::endl; */
    results->valid = true;
    StringStore* workingCopy = getRecoveryUnitBranch_forking(opCtx);
    auto it = workingCopy->lower_bound(_prefix);
    auto end = workingCopy->upper_bound(_postfix);
    size_t distance = workingCopy->distance(it, end);
    for (size_t i = 0; i < distance; i++) {
        std::string rec = it->second;
        size_t dataSize;
        RecordId rid;
        rid = RecordId(extractRecordId(it->first));
        RecordData rd = RecordData(rec.c_str(), rec.length() - 1);
        const Status status = adaptor->validate(rid, rd, &dataSize);
        if (!status.isOK()) {
            if (results->valid) {
                results->errors.push_back("detected one or more invalid documents (see logs)");
            }
            results->valid = false;
            log() << "Invalid object detected in " << _prefix << " with id" << std::to_string(i)
                  << ": " << status.reason();
        }
        ++it;
    }
    output->appendNumber("nrecords", distance);
    return Status::OK();
}

void RecordStore::appendCustomStats(OperationContext* opCtx,
                                    BSONObjBuilder* result,
                                    double scale) const {
    // TODO: Implement.
}

Status RecordStore::touch(OperationContext* opCtx, BSONObjBuilder* output) const {
    // TODO : implement.
    return Status::OK();
}

void RecordStore::waitForAllEarlierOplogWritesToBeVisible(OperationContext* opCtx) const {
    // TODO : implement.
}

void RecordStore::updateStatsAfterRepair(OperationContext* opCtx,
                                         long long numRecords,
                                         long long dataSize) {
    // TODO: Implement.
}

RecordStore::Cursor::Cursor(OperationContext* opCtx, const RecordStore& rs) : opCtx(opCtx) {
    _savedPosition = boost::none;
    _needFirstSeek = true;
    _ident = rs._ident;
    _prefix = rs._prefix;
    _postfix = rs._postfix;
    log() << "CREATE cursor for ident " << _ident << " with #elements:" << rs.numRecords(opCtx);
}

boost::optional<Record> RecordStore::Cursor::next() {
    /* std::cout << "Next" << std::endl; */
    _savedPosition = boost::none;
    StringStore* workingCopy = getRecoveryUnitBranch_forking(opCtx);
    /* std::cout << "Next" << std::endl; */
    if (_needFirstSeek) {
        _needFirstSeek = false;
        it = workingCopy->lower_bound(_prefix);
        /* if (it != workingCopy->end()) */
        /*     std::cout << "Starting at " << it->first << " with len " */
        /*               << std::to_string(it->first.length()) << std::endl; */
    } else if (it != workingCopy->end() && !_lastMoveWasRestore) {
        /* std::cout << "Moving off of item with len " << std::to_string(it->first.length()) */
        /* << std::endl; */
        /* std::cout << "iterating" << std::endl; */
        ++it;
    }
    _lastMoveWasRestore = false;
    if (it != workingCopy->end() && inPrefix(it->first)) {
        _savedPosition = std::string(it->first);
        /* std::cout << toHex(it->first.c_str(), it->first.length()) << std::endl; */
        return Record{RecordId(extractRecordId(it->first)),
                      RecordData(it->second.c_str(), it->second.length())};
    }
    /* std::cout << "NONE returned from next" << std::endl; */
    return boost::none;
}

boost::optional<Record> RecordStore::Cursor::seekExact(const RecordId& id) {
    /* std::cout << "seek xact to " << std::to_string(id.repr()) << std::endl; */
    _savedPosition = boost::none;
    _lastMoveWasRestore = false;
    StringStore* workingCopy = getRecoveryUnitBranch_forking(opCtx);
    std::string key = createKey(_ident, id.repr());
    it = workingCopy->find(key);
    if (it == workingCopy->end() || !inPrefix(it->first)) {
        return boost::none;
    }
    _needFirstSeek = false;
    _savedPosition = it->first;
    return Record{id, RecordData(it->second.c_str(), it->second.length()).getOwned()};
}

void RecordStore::Cursor::save() {
    /* std::cout << "Save" << std::endl; */
}

void RecordStore::Cursor::saveUnpositioned() {
    /* std::cout << "SaveU" << std::endl; */
}

bool RecordStore::Cursor::restore() {
    /* std::cout << "Restore" << std::endl; */
    StringStore* workingCopy = getRecoveryUnitBranch_forking(opCtx);
    it = (_savedPosition) ? workingCopy->lower_bound(_savedPosition.value()) : workingCopy->end();
    /* if (it != workingCopy->end()) */
    /*     std::cout << "restored" << toHex(it->first.c_str(), it->first.length()) << std::endl; */
    _lastMoveWasRestore = it == workingCopy->end() || it->first != _savedPosition.value();
    return true;
}

void RecordStore::Cursor::detachFromOperationContext() {
    invariant(opCtx != nullptr);
    opCtx = nullptr;
}

void RecordStore::Cursor::reattachToOperationContext(OperationContext* opCtx) {
    invariant(opCtx != nullptr);
    this->opCtx = opCtx;
}

bool RecordStore::Cursor::inPrefix(const std::string& key_string) {
    return (key_string >= _prefix) && (key_string <= _postfix);
}

// Reverse Cursor
RecordStore::ReverseCursor::ReverseCursor(OperationContext* opCtx, const RecordStore& rs)
    : opCtx(opCtx) {
    _savedPosition = boost::none;
    _ident = rs._ident;
    _prefix = rs._prefix;
    _postfix = rs._postfix;
}

boost::optional<Record> RecordStore::ReverseCursor::next() {
    _savedPosition = boost::none;
    StringStore* workingCopy = getRecoveryUnitBranch_forking(opCtx);
    if (_needFirstSeek) {
        _needFirstSeek = false;
        it = StringStore::reverse_iterator(workingCopy->upper_bound(_postfix));
    } else if (it != workingCopy->rend() && !_lastMoveWasRestore) {
        ++it;
    }

    _lastMoveWasRestore = false;

    if (it != workingCopy->rend() && inPrefix(it->first)) {
        _savedPosition = std::string(it->first);
        Record nextRecord;
        nextRecord.id = RecordId(extractRecordId(it->first));
        nextRecord.data = RecordData(it->second.c_str(), it->second.length());
        return nextRecord;
    }
    return boost::none;
}

boost::optional<Record> RecordStore::ReverseCursor::seekExact(const RecordId& id) {
    _needFirstSeek = false;
    _savedPosition = boost::none;
    StringStore* workingCopy = getRecoveryUnitBranch_forking(opCtx);
    std::string key = createKey(_ident, id.repr());
    StringStore::iterator canFind = workingCopy->find(key);
    if (canFind == workingCopy->end() || !inPrefix(canFind->first)) {
        it = workingCopy->rend();
        return boost::none;
    }
    it = StringStore::reverse_iterator(++canFind);  // reverse iterator returns item 1 before
    _savedPosition = it->first;
    return Record{id, RecordData(it->second.c_str(), it->second.length())};
}

void RecordStore::ReverseCursor::save() {}

void RecordStore::ReverseCursor::saveUnpositioned() {}

bool RecordStore::ReverseCursor::restore() {
    StringStore* workingCopy = getRecoveryUnitBranch_forking(opCtx);
    it = _savedPosition
        ? StringStore::reverse_iterator(workingCopy->upper_bound(_savedPosition.value()))
        : workingCopy->rend();
    _lastMoveWasRestore = (it == workingCopy->rend() || it->first != _savedPosition.value());
    return true;
}

void RecordStore::ReverseCursor::detachFromOperationContext() {
    invariant(opCtx != nullptr);
    opCtx = nullptr;
}

void RecordStore::ReverseCursor::reattachToOperationContext(OperationContext* opCtx) {
    invariant(opCtx != nullptr);
    this->opCtx = opCtx;
}

bool RecordStore::ReverseCursor::inPrefix(const std::string& key_string) {
    return (key_string >= _prefix) && (key_string <= _postfix);
}
}  // namespace biggie
}  // namespace mongo
