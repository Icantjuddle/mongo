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
#include "mongo/util/hex.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/shared_buffer.h"

#include <cstring>
#include <iomanip>
#include <memory>
#include <set>
#include <sstream>
#include <utility>

namespace mongo {
namespace biggie {
namespace {

// This function is the same as the one in record store--basically, using the git analogy, create
// a working branch if one does not exist.
StringStore* getRecoveryUnitBranch_forking(OperationContext* opCtx) {
    RecoveryUnit* biggieRCU = checked_cast<RecoveryUnit*>(opCtx->recoveryUnit());
    invariant(biggieRCU);
    biggieRCU->forkIfNeeded();
    return biggieRCU->getWorkingCopy();
}

// This just makes all the fields in a BSON object equal to "".
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

// This function converts a key and an ordering to a KeyString.
std::unique_ptr<KeyString> keyToKeyString(const BSONObj& key, Ordering order) {
    KeyString::Version version = KeyString::Version::V1;
    std::unique_ptr<KeyString> retKs = std::make_unique<KeyString>(version, key, order);
    return retKs;
}

// This combines a key, a record ID, and the ident into a single KeyString. Because we cannot
// compare keys properly (since they require an ordering, because we may have descending keys
// or multi-field keys), we need to convert them into a KeyString first, and then we can just
// compare them. Thus, we use a nested KeyString of keys inside our actual KeyString. The only
// difference between this function and the one below is that this one calls resetToKey first.
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

// This is similar to the function above, but it returns a string instead of a KeyString. The
// reason we need both is that we also need to store the typebits, and therefore, we occasionally
// need to return the KeyString (in the function above). However, most of the time the string
// representation of the KeyString is good enough, and therefore we just return the string (this
// function).
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

// This function converts a KeyString into an IndexKeyEntry. We don't need to store the typebits
// for the outer key string (the one consisting of the prefix, the key, and the recordId) since
// those will not be used. However, we do need to store the typebits for the internal keystring
// (made from the key itself), as those typebits are potentially important.
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

    // First we get the BSONObj key.
    SharedBuffer sb;
    int counter = 0;
    for (auto&& elem : bsonObj) {
        // The key is the second field.
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

// We append \1 to all idents we get, and therefore the KeyString with ident + \0 will only be
// before elements in this ident, and the KeyString with ident + \2 will only be after elements in
// this ident.
SortedDataInterface::SortedDataInterface(const Ordering& ordering, bool isUnique, StringData ident)
    : _order(ordering),
      // All entries in this ident will have a prefix of ident + \1.
      _prefix(ident.toString().append(1, '\1')),
      // Therefore, the string ident + \2 will be greater than all elements in this ident.
      _postfix(ident.toString().append(1, '\2')),
      isUnique(isUnique) {
    // This is the string representation of the KeyString before elements in this ident, which is
    // ident + \0. This is before all elements in this ident.
    _prefixBSON =
        combineKeyAndRID(BSONObj(), RecordId::min(), ident.toString().append(1, '\0'), ordering);
    // Similarly, this is the string representation of the KeyString for something greater than
    // all other elements in this ident.
    _postfixBSON = combineKeyAndRID(BSONObj(), RecordId::min(), _postfix, ordering);
}

Status SortedDataInterface::insert(OperationContext* opCtx,
                                   const BSONObj& key,
                                   const RecordId& loc,
                                   bool dupsAllowed) {
    // The KeyString representation of the key.
    std::unique_ptr<KeyString> workingCopyInternalKs = keyToKeyString(key, _order);
    // The KeyString of prefix (which is ident + \1), key, loc.
    std::unique_ptr<KeyString> workingCopyOuterKs = combineKeyAndRIDKS(key, loc, _prefix, _order);
    // The string representation.
    std::string workingCopyInsertKey = combineKeyAndRID(key, loc, _prefix, _order);

    StringStore* workingCopy = getRecoveryUnitBranch_forking(opCtx);

    if (workingCopy->find(workingCopyInsertKey) != workingCopy->end()) {
        return Status::OK();
    }

    // If dups are not allowed, then we need to check that we are not inserting something with an
    // existing key but a different recordId. However, if the combination of key, recordId already
    // exists, then we are fine, since we are allowed to insert duplicates.
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

    // The key we insert is the workingCopyOuterKs as described above. The value is the typebits
    // for the internal keystring (created from the key/order), which we will use when decoding the
    // key and creating an IndexKeyEntry.
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

// This function is, as of now, not in the interface, but there exists a server ticket to add
// truncate to the list of commands able to be used.
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

    // We effectively do the same check as in insert. However, we also check to make sure that
    // the iterator returned to us by lower_bound also happens to be inside out ident.
    if (workingCopy->find(workingCopyCheckKey) != workingCopy->end()) {
        return Status::OK();
    }

    if (!dupsAllowed) {
        std::string workingCopyLowerBound = combineKeyAndRID(key, RecordId::min(), _prefix, _order);
        auto lowerBoundIterator = workingCopy->lower_bound(workingCopyLowerBound);

        if (lowerBoundIterator != workingCopy->end() &&
            lowerBoundIterator->first != workingCopyCheckKey &&
            lowerBoundIterator->first.compare(_postfixBSON) < 0 &&
            lowerBoundIterator->first.compare(
                combineKeyAndRID(key, RecordId::max(), _prefix, _order)) <= 0) {
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
        ++it;
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

    return std::make_unique<SortedDataInterface::Cursor>(opCtx,
                                                         isForward,
                                                         _prefix,
                                                         _postfix,
                                                         workingCopy,
                                                         _order,
                                                         isUnique,
                                                         _prefixBSON,
                                                         _postfixBSON);
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
                                    bool isUnique,
                                    std::string prefixBSON,
                                    std::string postfixBSON)
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
      isUnique(isUnique),
      _prefixBSON(prefixBSON),
      _postfixBSON(postfixBSON) {}

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
    // If forward and inclusive or reverse and not inclusive, then we use the last element in this
    // ident. Otherwise, we use the first as our bound.
    if (_forward == inclusive) {
        endPosBound = combineKeyAndRID(finalKey, RecordId::max(), _prefix, _order);
    } else {
        endPosBound = combineKeyAndRID(finalKey, RecordId::min(), _prefix, _order);
    }
    if (_forward) {
        endPos = workingCopy->lower_bound(endPosBound);
    } else {
        // Reverse iterators work with upper bound since upper bound will return the first element
        // past the argument, so when it becomes a reverse iterator, it goes backwards one,
        // (according to the C++ standard) and we end up in the right place.
        endPosReverse = StringStore::reverse_iterator(workingCopy->upper_bound(endPosBound));
    }
}

boost::optional<IndexKeyEntry> SortedDataInterface::Cursor::next(RequestedInfo parts) {
    if (!atEOF) {
        // If the last move was restore, then we don't need to advance the cursor, since the user
        // never got the value the cursor was pointing to in the first place. However,
        // lastMoveWasRestore will go through extra logic on a unique index, since unique indexes
        // are not allowed to return the same key twice.
        if (lastMoveWasRestore) {
            lastMoveWasRestore = false;
        } else {
            // We basically just check to make sure next is in the ident.
            if (_forward && forwardIt != workingCopy->end()) {
                ++forwardIt;
            } else if (reverseIt != workingCopy->rend()) {
                ++reverseIt;
            }
            // We check here to make sure that we are on the correct side of the end position.
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

    // Here, we check to make sure the iterator is in the ident.
    if ((_forward &&
         (forwardIt == workingCopy->end() || forwardIt->first.compare(_postfixBSON) >= 0)) ||
        (!_forward &&
         (reverseIt == workingCopy->rend() || reverseIt->first.compare(_prefixBSON) <= 0))) {
        atEOF = true;
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

    // Similar to above, if forward and inclusive or reverse and not inclusive, then use min() for
    // recordId. Else, we should use max().
    if (_forward == inclusive) {
        workingCopyBound = combineKeyAndRID(finalKey, RecordId::min(), _prefix, _order);
    } else {
        workingCopyBound = combineKeyAndRID(finalKey, RecordId::max(), _prefix, _order);
    }

    if (finalKey.isEmpty()) {
        // If the key is empty and it's not inclusive, then no elements satisfy this seek.
        if (!inclusive) {
            atEOF = true;
            return boost::none;
        } else {
            // Otherwise, we just try to find the first element in this ident.
            if (_forward) {
                forwardIt = workingCopy->lower_bound(workingCopyBound);
            } else {

                // Reverse iterators work with upper bound since upper bound will return the first
                // element past the argument, so when it becomes a reverse iterator, it goes
                // backwards one, (according to the C++ standard) and we end up in the right place.
                reverseIt =
                    StringStore::reverse_iterator(workingCopy->upper_bound(workingCopyBound));
            }
            // Here, we check to make sure the iterator doesn't fall off the data structure and is
            // in the ident. We also check to make sure it is on the correct side of the end
            // position, if it was set.
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
        // Otherwise, we seek to the nearest element to our key, but only to the right.
        if (_forward) {
            forwardIt = workingCopy->lower_bound(workingCopyBound);
        } else {
            // Reverse iterators work with upper bound since upper bound will return the first
            // element past the argument, so when it becomes a reverse iterator, it goes
            // backwards one, (according to the C++ standard) and we end up in the right place.
            reverseIt = StringStore::reverse_iterator(workingCopy->upper_bound(workingCopyBound));
        }
        // Once again, we check to make sure the iterator didn't fall off the data structure and
        // still is in the ident.
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

    // Everything checks out, so we have successfullly seeked and now return.
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

    // Here, we have to reset the end position if one was set earlier.
    if (endPosValid) {
        setEndPosition(endPosKey.get(), endPosIncl);
    }

    // We reset the cursor, and make sure it's within the end position bounds. It doesn't matter if
    // the cursor is not in the ident right now, since that will be taken care of upon the call to
    // next().
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
        } else {
            // Unique indices cannot return the same key twice. Therefore, if we would normally not
            // advance on the next call to next() by setting lastMoveWasRestore, we potentially
            // won't set it if that would cause us to return the same value twice.
            int twoKeyCmp = compareTwoKeys(
                forwardIt->first, forwardIt->second, saveKey, forwardIt->second, _order);
            lastMoveWasRestore = (twoKeyCmp != 0);
        }

    } else {
        // Now we are dealing with reverse cursors, and use similar logic.
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
        } else {
            // We use similar logic for reverse cursors on unique indexes.
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
