/**
 * Copyright (C) 2018 MongoDB Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects
 * for all of the code used other than as permitted herein. If you modify
 * file(s) with this exception, you may extend this exception to your
 * version of the file(s), but you are not obligated to do so. If you do not
 * wish to do so, delete this exception statement from your version. If you
 * delete this exception statement from all source files in the program,
 * then also delete it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/db/storage/biggie/biggie_sorted_impl.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/catalog/index_catalog_entry.h"
#include "mongo/db/storage/biggie/biggie_recovery_unit.h"
#include "mongo/db/storage/biggie/store.h"
#include "mongo/db/storage/index_entry_comparison.h"
#include "mongo/db/storage/key_string.h"
#include "mongo/platform/basic.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/bufreader.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/shared_buffer.h"

#include "mongo/util/hex.h"

#include <cstring>
#include <iomanip>
#include <memory>
#include <set>
#include <sstream>
#include <utility>

namespace mongo {
namespace biggie {
namespace {

StringStore* getRecoveryUnitBranch_forking(OperationContext* opCtx) {
    RecoveryUnit* biggieRCU = checked_cast<RecoveryUnit*>(opCtx->recoveryUnit());
    invariant(biggieRCU);
    biggieRCU->forkIfNeeded();
    return biggieRCU->getWorkingCopy();
}

BSONObj stripFieldNames(const BSONObj& obj) {
    BSONObjIterator it(obj);
    BSONObjBuilder bob;
    while (it.more()) {
        bob.appendAs(it.next(), "");
    }
    return bob.obj();
}

Status dupKeyError(const BSONObj& key) {
    StringBuilder sb;
    sb << "E11000 duplicate key error ";
    sb << "dup key: " << key;
    return Status(ErrorCodes::DuplicateKey, sb.str());
}

std::unique_ptr<KeyString> keyToKeyString(const BSONObj& key, Ordering order) {
    KeyString::Version version = KeyString::Version::V1;
    std::unique_ptr<KeyString> retKs = std::make_unique<KeyString>(version, key, order);
    return retKs;
}

std::string combineKeyAndRIDWithReset(const BSONObj& key,
                                      const RecordId& loc,
                                      std::string prefixToUse,
                                      Ordering order) {
    KeyString::Version version = KeyString::Version::V1;
    std::unique_ptr<KeyString> ks = std::make_unique<KeyString>(version);
    ks->resetToKey(key, order);

    BSONObjBuilder b;
    b.append("", prefixToUse);                                  // prefix
    b.append("", std::string(ks->getBuffer(), ks->getSize()));  // key

    Ordering allAscending = Ordering::make(BSONObj());
    std::unique_ptr<KeyString> retKs =
        std::make_unique<KeyString>(version, b.obj(), allAscending, loc);
    return std::string(retKs->getBuffer(), retKs->getSize());
}

std::unique_ptr<KeyString> combineKeyAndRIDKS(const BSONObj& key,
                                              const RecordId& loc,
                                              std::string prefixToUse,
                                              Ordering order) {
    KeyString::Version version = KeyString::Version::V1;
    KeyString ks(version, key, order);
    BSONObjBuilder b;
    b.append("", prefixToUse);                                // prefix
    b.append("", std::string(ks.getBuffer(), ks.getSize()));  // key
    Ordering allAscending = Ordering::make(BSONObj());
    return std::make_unique<KeyString>(version, b.obj(), allAscending, loc);
}

std::string combineKeyAndRID(const BSONObj& key,
                             const RecordId& loc,
                             std::string prefixToUse,
                             Ordering order) {
    KeyString::Version version = KeyString::Version::V1;
    KeyString ks(version, key, order);

    BSONObjBuilder b;
    b.append("", prefixToUse);                                // prefix
    b.append("", std::string(ks.getBuffer(), ks.getSize()));  // key
    Ordering allAscending = Ordering::make(BSONObj());
    std::unique_ptr<KeyString> retKs =
        std::make_unique<KeyString>(version, b.obj(), allAscending, loc);
    return std::string(retKs->getBuffer(), retKs->getSize());
}


IndexKeyEntry keyStringToIndexKeyEntry(std::string keyString,
                                       std::string typeBitsString,
                                       Ordering order) {
    KeyString::Version version = KeyString::Version::V1;
    KeyString::TypeBits tbInternal = KeyString::TypeBits(version);
    KeyString::TypeBits tbOuter = KeyString::TypeBits(version);

    BufReader brTbInternal(typeBitsString.c_str(), typeBitsString.length());
    tbInternal.resetFromBuffer(&brTbInternal);

    Ordering allAscending = Ordering::make(BSONObj());

    BSONObj bsonObj =
        KeyString::toBsonSafe(keyString.c_str(), keyString.length(), allAscending, tbOuter);

    // first getting the BSONObj key
    SharedBuffer sb;

    int counter = 0;
    for (auto&& elem : bsonObj) {
        // key is the second field
        if (counter == 1) {
            const char* valStart = elem.valuestr();
            int valSize = elem.valuestrsize();
            KeyString ks(version);
            ks.resetFromBuffer(valStart, valSize);

            BSONObj originalKey =
                KeyString::toBsonSafe(ks.getBuffer(), ks.getSize(), order, tbInternal);

            sb = SharedBuffer::allocate(originalKey.objsize());
            std::memcpy(sb.get(), originalKey.objdata(), originalKey.objsize());
            break;
        }
        counter++;
    }
    RecordId rid = KeyString::decodeRecordIdAtEnd(keyString.c_str(), keyString.length());
    ConstSharedBuffer csb(sb);
    BSONObj key(csb);

    return IndexKeyEntry(key, rid);
}

int compareTwoKeys(
    std::string ks1, std::string tbs1, std::string ks2, std::string tbs2, Ordering order) {
    size_t size1 = KeyString::sizeWithoutRecordIdAtEnd(ks1.c_str(), ks1.length());
    size_t size2 = KeyString::sizeWithoutRecordIdAtEnd(ks2.c_str(), ks2.length());
    auto cmpSmallerMemory = std::memcmp(ks1.c_str(), ks2.c_str(), std::min(size1, size2));

    if (cmpSmallerMemory != 0) {
        return cmpSmallerMemory;
    }
    if (size1 == size2) {
        return 0;
    }
    return (size1 > size2);
}

}  // namepsace

SortedDataBuilderInterface::SortedDataBuilderInterface(OperationContext* opCtx,
                                                       bool dupsAllowed,
                                                       Ordering order,
                                                       std::string prefix,
                                                       std::string postfix)
    : opCtx(opCtx),
      dupsAllowed(dupsAllowed),
      order(order),
      prefix(prefix),
      postfix(postfix),
      hasLast(false),
      lastKeyToString(""),
      lastRID(-1) {}

void SortedDataBuilderInterface::commit(bool mayInterrupt) {
    biggie::RecoveryUnit* ru = checked_cast<biggie::RecoveryUnit*>(opCtx->recoveryUnit());
    ru->forkIfNeeded();
    ru->commitUnitOfWork();
}

Status SortedDataBuilderInterface::addKey(const BSONObj& key, const RecordId& loc) {
    StringStore* workingCopy = getRecoveryUnitBranch_forking(opCtx);

    invariant(loc.isNormal());

    std::unique_ptr<KeyString> newKS = keyToKeyString(key, order);
    std::string newKSToString = std::string(newKS->getBuffer(), newKS->getSize());

    int twoKeyCmp = 1;
    int twoRIDCmp = 1;

    if (hasLast) {
        twoKeyCmp = newKSToString.compare(lastKeyToString);
        twoRIDCmp = loc.repr() - lastRID;
    }

    if (twoKeyCmp < 0 || (dupsAllowed && twoKeyCmp == 0 && twoRIDCmp < 0)) {
        return Status(ErrorCodes::InternalError,
                      "expected ascending (key, RecordId) order in bulk builder");
    }
    if (!dupsAllowed && twoKeyCmp == 0 && twoRIDCmp != 0) {
        return dupKeyError(key);
    }

    std::string workingCopyInsertKey = combineKeyAndRID(key, loc, prefix, order);
    std::unique_ptr<KeyString> workingCopyInternalKs = keyToKeyString(key, order);
    std::unique_ptr<KeyString> workingCopyOuterKs = combineKeyAndRIDKS(key, loc, prefix, order);

    std::string internalTbString(
        reinterpret_cast<const char*>(workingCopyInternalKs->getTypeBits().getBuffer()),
        workingCopyInternalKs->getTypeBits().getSize());

    workingCopy->insert(StringStore::value_type(workingCopyInsertKey, internalTbString));

    hasLast = true;
    lastKeyToString = newKSToString;
    lastRID = loc.repr();

    return Status::OK();
}

SortedDataBuilderInterface* SortedDataInterface::getBulkBuilder(OperationContext* opCtx,
                                                                bool dupsAllowed) {
    return new SortedDataBuilderInterface(opCtx, dupsAllowed, _order, _prefix, _postfix);
}

SortedDataInterface::SortedDataInterface(const Ordering& ordering, bool isUnique, StringData ident)
    : _order(ordering),
      _prefix(ident.toString().append(1, '\0').append(1, '\1')),
      _postfix(ident.toString().append(2, '\1')),
      isUnique(isUnique) {
    _prefixBSON = combineKeyAndRID(BSONObj(), RecordId::min(), ident.toString().append(2, '\0'), ordering);
    _postfixBSON = combineKeyAndRID(BSONObj(), RecordId::min(), _postfix, ordering);

    // start printing stuff here
    //

    /*

    std::string ident1 = "index-6--8250259195595751598";
    std::string ident2 = "index-8--8250259195595751598";

    std::string _prefix1 = std::string(ident1).append(1, '\1');
    std::string _prefix2 = std::string(ident2).append(1, '\1');
    std::string _postfix1 = std::string(ident1).append(1, '\2');
    std::string _postfix2 = std::string(ident2).append(1, '\2');

    std::string _prefixBSON1 = combineKeyAndRID(BSONObj(), RecordId::min(), std::string(ident1).append(1, '\0'), ordering);
    std::string _prefixBSON2 = combineKeyAndRID(BSONObj(), RecordId::min(), std::string(ident2).append(1, '\0'), ordering);
    std::string _postfixBSON1 = combineKeyAndRID(BSONObj(), RecordId::max(), _postfix1, ordering);
    std::string _postfixBSON2 = combineKeyAndRID(BSONObj(), RecordId::max(), _postfix2, ordering);

    std::string _prefixInsertSample1 = combineKeyAndRID(BSONObj(), RecordId(1), _prefix1,  ordering);
    std::string _prefixInsertSample2 = combineKeyAndRID(BSONObj(), RecordId(1), _prefix2,  ordering);


    log() << "Ident:" << ident1 << "\n\n";
    log() << "PrefixBSON:" << toHex(_prefixBSON1.c_str(), _prefixBSON1.length()) << "\n\n";
    log() << "Prefix use:" << toHex(_prefixInsertSample1.c_str(), _prefixInsertSample1.length()) << "\n\n";
    log() << "postfixBSON:" << toHex(_postfixBSON1.c_str(), _postfixBSON1.length()) << "\n\n";

    log() << "Ident:" << ident2 << "\n\n";
    log() << "PrefixBSON:" << toHex(_prefixBSON2.c_str(), _prefixBSON2.length()) << "\n\n";
    log() << "Prefix use:" << toHex(_prefixInsertSample2.c_str(), _prefixInsertSample2.length()) << "\n\n";
    log() << "postfixBSON:" << toHex(_postfixBSON2.c_str(), _postfixBSON2.length()) << "\n\n";

    */
}

Status SortedDataInterface::insert(OperationContext* opCtx,
                                   const BSONObj& key,
                                   const RecordId& loc,
                                   bool dupsAllowed) {
    std::unique_ptr<KeyString> workingCopyInternalKs = keyToKeyString(key, _order);
    std::unique_ptr<KeyString> workingCopyOuterKs = combineKeyAndRIDKS(key, loc, _prefix, _order);
    std::string workingCopyInsertKey = combineKeyAndRID(key, loc, _prefix, _order);

    StringStore* workingCopy = getRecoveryUnitBranch_forking(opCtx);

    if (workingCopy->find(workingCopyInsertKey) != workingCopy->end()) {
        return Status::OK();
    }

    if (!dupsAllowed) {
        std::string workingCopyLowerBound = combineKeyAndRID(key, RecordId::min(), _prefix, _order);
        std::string workingCopyUpperBound = combineKeyAndRID(key, RecordId::max(), _prefix, _order);
        StringStore::iterator lowerBoundIterator = workingCopy->lower_bound(workingCopyLowerBound);
        StringStore::iterator upperBoundIterator = workingCopy->upper_bound(workingCopyUpperBound);

        if (lowerBoundIterator != workingCopy->end()) {
            IndexKeyEntry ike = keyStringToIndexKeyEntry(
                lowerBoundIterator->first, lowerBoundIterator->second, _order);
            if (ike.key.toString() == key.toString() && ike.loc.repr() != loc.repr()) {
                return dupKeyError(key);
            }
        }
    }

    std::string internalTbString =
        std::string(reinterpret_cast<const char*>(workingCopyInternalKs->getTypeBits().getBuffer()),
                    workingCopyInternalKs->getTypeBits().getSize());
    workingCopy->insert(StringStore::value_type(workingCopyInsertKey, internalTbString));
    return Status::OK();
}
void SortedDataInterface::unindex(OperationContext* opCtx,
                                  const BSONObj& key,
                                  const RecordId& loc,
                                  bool dupsAllowed) {
    std::string workingCopyInsertKey = combineKeyAndRID(key, loc, _prefix, _order);
    StringStore* workingCopy = getRecoveryUnitBranch_forking(opCtx);
    workingCopy->erase(workingCopyInsertKey);
}
Status SortedDataInterface::truncate(OperationContext* opCtx) {
    StringStore* workingCopy = getRecoveryUnitBranch_forking(opCtx);
    auto workingCopyLowerBound = workingCopy->lower_bound(_prefixBSON);
    auto workingCopyUpperBound = workingCopy->upper_bound(_postfixBSON);
    workingCopy->erase(workingCopyLowerBound, workingCopyUpperBound);
    return Status::OK();
}
Status SortedDataInterface::dupKeyCheck(OperationContext* opCtx,
                                        const BSONObj& key,
                                        const RecordId& loc) {
    std::string workingCopyCheckKey = combineKeyAndRID(key, loc, _prefix, _order);
    StringStore* workingCopy = getRecoveryUnitBranch_forking(opCtx);

    if (workingCopy->find(workingCopyCheckKey) != workingCopy->end()) {
        return Status::OK();
    }

    if (!dupsAllowed) {
        std::string workingCopyLowerBound = combineKeyAndRID(key, RecordId::min(), _prefix, _order);
        auto lowerBoundIterator = workingCopy->lower_bound(workingCopyLowerBound);

        if (lowerBoundIterator != workingCopy->end() &&
            lowerBoundIterator->first != workingCopyCheckKey) {
            return dupKeyError(key);
        }
    }
    return Status::OK();
}

void SortedDataInterface::fullValidate(OperationContext* opCtx,
                                       long long* numKeysOut,
                                       ValidateResults* fullResults) const {
    StringStore* workingCopy = getRecoveryUnitBranch_forking(opCtx);
    long long numKeys = 0;
    auto it = workingCopy->lower_bound(_prefixBSON);
    while (it != workingCopy->end() && it->first.compare(_postfixBSON) < 0) {
        it = ++it;
        numKeys++;
    }
    *numKeysOut = numKeys;
}

bool SortedDataInterface::appendCustomStats(OperationContext* opCtx,
                                            BSONObjBuilder* output,
                                            double scale) const {
    return false;
}
long long SortedDataInterface::getSpaceUsedBytes(OperationContext* opCtx) const {
    StringStore* str = getRecoveryUnitBranch_forking(opCtx);
    size_t totalSize = 0;
    StringStore::iterator it = str->lower_bound(_prefixBSON);
    StringStore::iterator end = str->upper_bound(_postfixBSON);
    int64_t numElements = str->distance(it, end);
    for (int i = 0; i < numElements; i++) {
        totalSize += it->first.length();
        ++it;
    }
    return (long long)totalSize;
}
bool SortedDataInterface::isEmpty(OperationContext* opCtx) {
    StringStore* workingCopy = getRecoveryUnitBranch_forking(opCtx);
    return workingCopy->distance(workingCopy->lower_bound(_prefixBSON),
                                 workingCopy->upper_bound(_postfixBSON)) == 0;
}

std::unique_ptr<mongo::SortedDataInterface::Cursor> SortedDataInterface::newCursor(
    OperationContext* opCtx, bool isForward) const {
    StringStore* workingCopy = getRecoveryUnitBranch_forking(opCtx);

    return std::make_unique<SortedDataInterface::Cursor>(
        opCtx, isForward, _prefix, _postfix, workingCopy, _order, isUnique);
}
Status SortedDataInterface::initAsEmpty(OperationContext* opCtx) {
    return Status::OK();
}

// Cursor
SortedDataInterface::Cursor::Cursor(OperationContext* opCtx,
                                    bool isForward,
                                    std::string _prefix,
                                    std::string _postfix,
                                    StringStore* workingCopy,
                                    Ordering order,
                                    bool isUnique)
    : opCtx(opCtx),
      workingCopy(workingCopy),
      endPos(boost::none),
      endPosReverse(boost::none),
      endPosValid(false),
      _forward(isForward),
      atEOF(false),
      lastMoveWasRestore(false),
      _prefix(_prefix),
      _postfix(_postfix),
      forwardIt(workingCopy->begin()),
      reverseIt(workingCopy->rbegin()),
      _order(order),
      endPosIncl(false),
      isUnique(isUnique) {
    std::string preFix = std::string(_prefix);
    preFix[preFix.length() - 1] = '\0';
    _prefixBSON = combineKeyAndRID(BSONObj(), RecordId::min(), preFix, order);
    _postfixBSON = combineKeyAndRID(BSONObj(), RecordId::min(), _postfix, order);
}

void SortedDataInterface::Cursor::setEndPosition(const BSONObj& key, bool inclusive) {
    auto finalKey = stripFieldNames(key);
    StringStore* workingCopy = getRecoveryUnitBranch_forking(opCtx);
    if (finalKey.isEmpty()) {
        endPosValid = false;
        endPos = boost::none;
        return;
    }
    endPosValid = true;
    endPosIncl = inclusive;
    endPosKey = key;
    std::string endPosBound;
    if (_forward == inclusive) {
        endPosBound = combineKeyAndRID(finalKey, RecordId::max(), _prefix, _order);
    } else {
        endPosBound = combineKeyAndRID(finalKey, RecordId::min(), _prefix, _order);
    }
    if (_forward) {
        endPos = workingCopy->lower_bound(endPosBound);
    } else {
        endPosReverse = StringStore::reverse_iterator(workingCopy->upper_bound(endPosBound));
    }
}

boost::optional<IndexKeyEntry> SortedDataInterface::Cursor::next(RequestedInfo parts) {
    if (!atEOF) {
        if (lastMoveWasRestore) {
            lastMoveWasRestore = false;
        } else {
            if (_forward && forwardIt != workingCopy->end()) {
                ++forwardIt;
            } else if (reverseIt != workingCopy->rend()) {
                ++reverseIt;
            }
            if ((!_forward && (reverseIt == workingCopy->rend() ||
                               (endPosValid && reverseIt->first == endPosReverse.get()->first))) ||
                (_forward && (forwardIt == workingCopy->end() ||
                              (endPosValid && forwardIt->first == endPos.get()->first)))) {
                atEOF = true;
                return boost::none;
            }
        }
    } else {
        lastMoveWasRestore = false;
        return boost::none;
    }

    if (_forward) {
        return keyStringToIndexKeyEntry(forwardIt->first, forwardIt->second, _order);
    }
    return keyStringToIndexKeyEntry(reverseIt->first, reverseIt->second, _order);
}

boost::optional<IndexKeyEntry> SortedDataInterface::Cursor::seekAfterProcessing(BSONObj finalKey,
                                                                                bool inclusive) {
    std::string workingCopyBound;

    if (_forward == inclusive) {
        workingCopyBound = combineKeyAndRID(finalKey, RecordId::min(), _prefix, _order);
    } else {
        workingCopyBound = combineKeyAndRID(finalKey, RecordId::max(), _prefix, _order);
    }

    if (finalKey.isEmpty()) {
        if (!inclusive) {
            atEOF = true;
            return boost::none;
        } else {
            if (_forward) {
                forwardIt = workingCopy->lower_bound(workingCopyBound);
            } else {
                reverseIt =
                    StringStore::reverse_iterator(workingCopy->upper_bound(workingCopyBound));
            }
            if (((_forward && (forwardIt == workingCopy->end() ||
                               forwardIt->first.compare(_postfixBSON) > 0)) ||
                 (!_forward && (reverseIt == workingCopy->rend() ||
                                reverseIt->first.compare(_prefixBSON) < 0))) ||
                (endPosValid && ((_forward && endPos.get() != workingCopy->end() &&
                                  forwardIt->first.compare(endPos.get()->first) >= 0) ||
                                 (!_forward && endPosReverse.get() != workingCopy->rend() &&
                                  reverseIt->first.compare(endPos.get()->first) <= 0)))) {
                atEOF = true;
                return boost::none;
            }
        }
    } else {
        if (_forward) {
            forwardIt = workingCopy->lower_bound(workingCopyBound);
        } else {
            reverseIt = StringStore::reverse_iterator(workingCopy->upper_bound(workingCopyBound));
        }
        if (((_forward &&
              ((forwardIt == workingCopy->end() || forwardIt->first.compare(_postfixBSON) > 0) ||
               (endPosValid && endPos.get() != workingCopy->end() &&
                forwardIt->first.compare(endPos.get()->first) >= 0)))) ||
            ((!_forward &&
              ((reverseIt == workingCopy->rend() || reverseIt->first.compare(_prefixBSON) < 0) ||
               (endPosValid && endPosReverse.get() != workingCopy->rend() &&
                reverseIt->first.compare(endPosReverse.get()->first) <= 0))) ||
             false)) {
            atEOF = true;
            return boost::none;
        }
    }

    if (_forward) {
        return keyStringToIndexKeyEntry(forwardIt->first, forwardIt->second, _order);
    }
    return keyStringToIndexKeyEntry(reverseIt->first, reverseIt->second, _order);
}

boost::optional<IndexKeyEntry> SortedDataInterface::Cursor::seek(const BSONObj& key,
                                                                 bool inclusive,
                                                                 RequestedInfo parts) {
    BSONObj finalKey = stripFieldNames(key);
    lastMoveWasRestore = false;
    atEOF = false;

    return seekAfterProcessing(finalKey, inclusive);
}

boost::optional<IndexKeyEntry> SortedDataInterface::Cursor::seek(const IndexSeekPoint& seekPoint,
                                                                 RequestedInfo parts) {
    const BSONObj key = IndexEntryComparison::makeQueryObject(seekPoint, _forward);
    atEOF = false;
    bool inclusive = true;
    BSONObj finalKey = key;
    lastMoveWasRestore = false;

    return seekAfterProcessing(finalKey, inclusive);
}

void SortedDataInterface::Cursor::save() {
    atEOF = false;
    if (lastMoveWasRestore) {
        return;
    } else if (_forward && forwardIt != workingCopy->end()) {
        saveKey = forwardIt->first, forwardIt->second;
    } else if (!_forward && reverseIt != workingCopy->rend()) {  // reverse
        saveKey = reverseIt->first, reverseIt->second;
    } else {
        saveKey = "";
    }
}

void SortedDataInterface::Cursor::restore() {
    StringStore* workingCopy = getRecoveryUnitBranch_forking(opCtx);

    this->workingCopy = workingCopy;

    // end position
    if (endPosValid) {
        setEndPosition(endPosKey.get(), endPosIncl);
    }

    if (_forward) {
        if (saveKey.length() == 0) {
            forwardIt = workingCopy->end();
        } else {
            forwardIt = workingCopy->lower_bound(saveKey);
        }
        if ((forwardIt == workingCopy->end()) ||
            ((endPosValid && endPos.get() != workingCopy->end()) &&
             (forwardIt->first.compare(endPos.get()->first) >= 0))) {
            atEOF = true;
            lastMoveWasRestore = true;
            return;
        }

        if (!isUnique) {
            lastMoveWasRestore = (forwardIt->first.compare(saveKey) != 0);
        } else {  // unique
            int twoKeyCmp = compareTwoKeys(
                forwardIt->first, forwardIt->second, saveKey, forwardIt->second, _order);
            lastMoveWasRestore = (twoKeyCmp != 0);
        }

    } else {  // reverse cursor
        if (saveKey.length() == 0) {
            reverseIt = workingCopy->rend();
        } else {
            reverseIt = StringStore::reverse_iterator(workingCopy->upper_bound(saveKey));
        }
        if ((reverseIt == workingCopy->rend()) ||
            ((endPosValid && endPosReverse.get() != workingCopy->rend()) &&
             (reverseIt->first.compare(endPosReverse.get()->first) <= 0))) {
            atEOF = true;
            lastMoveWasRestore = true;
            return;
        }

        if (!isUnique) {
            lastMoveWasRestore = (reverseIt->first.compare(saveKey) != 0);
        } else {  // unique
            int twoKeyCmp = compareTwoKeys(
                reverseIt->first, reverseIt->second, saveKey, reverseIt->second, _order);
            lastMoveWasRestore = (twoKeyCmp != 0);
        }
    }
}

void SortedDataInterface::Cursor::detachFromOperationContext() {
    opCtx = nullptr;
}

void SortedDataInterface::Cursor::reattachToOperationContext(OperationContext* opCtx) {
    this->opCtx = opCtx;
}
}  // namespace biggie
}  // namespace mongo
