// biggie_sorted_impl.h

/**
 *    Copyright (C) 2018 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
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
#pragma once

#include "mongo/db/storage/biggie/store.h"
#include "mongo/db/storage/key_string.h"
#include "mongo/db/storage/sorted_data_interface.h"

namespace mongo {
namespace biggie {

class SortedDataBuilderInterface : public ::mongo::SortedDataBuilderInterface {
public:
    SortedDataBuilderInterface(OperationContext* opCtx,
                               bool dupsAllowed,
                               Ordering order,
                               std::string prefix,
                               std::string postfix);
    void commit(bool mayInterrupt) override;
    virtual Status addKey(const BSONObj& key, const RecordId& loc);
    OperationContext* opCtx;
    bool dupsAllowed;
    Ordering order;
    std::string prefix;
    std::string postfix;
    bool hasLast;
    std::string lastKeyToString;
    int64_t lastRID;
};

class SortedDataInterface : public ::mongo::SortedDataInterface {
public:
    // All the following public functions just implement the interface.
    SortedDataInterface(const Ordering& ordering, bool isUnique, StringData ident);
    virtual SortedDataBuilderInterface* getBulkBuilder(OperationContext* opCtx, bool dupsAllowed);
    virtual Status insert(OperationContext* opCtx,
                          const BSONObj& key,
                          const RecordId& loc,
                          bool dupsAllowed);
    virtual void unindex(OperationContext* opCtx,
                         const BSONObj& key,
                         const RecordId& loc,
                         bool dupsAllowed);
    virtual Status dupKeyCheck(OperationContext* opCtx, const BSONObj& key, const RecordId& loc);
    virtual void fullValidate(OperationContext* opCtx,
                              long long* numKeysOut,
                              ValidateResults* fullResults) const;
    virtual bool appendCustomStats(OperationContext* opCtx,
                                   BSONObjBuilder* output,
                                   double scale) const;
    virtual long long getSpaceUsedBytes(OperationContext* opCtx) const;
    virtual bool isEmpty(OperationContext* opCtx);
    virtual std::unique_ptr<mongo::SortedDataInterface::Cursor> newCursor(
        OperationContext* opCtx, bool isForward = true) const override;
    virtual Status initAsEmpty(OperationContext* opCtx);

    // The default is a forward Cursor.
    class Cursor final : public ::mongo::SortedDataInterface::Cursor {
    public:
        // All the following public functions just implement the interface.
        Cursor(OperationContext* opCtx,
               bool isForward,
               // This is the ident.
               std::string _prefix,
               // This is a string immediately after the ident and before other idents.
               std::string _postfix,
               StringStore* workingCopy,
               Ordering order,
               bool isUnique);
        virtual void setEndPosition(const BSONObj& key, bool inclusive) override;
        virtual boost::optional<IndexKeyEntry> next(RequestedInfo parts = kKeyAndLoc);
        virtual boost::optional<IndexKeyEntry> seek(const BSONObj& key,
                                                    bool inclusive,
                                                    RequestedInfo parts = kKeyAndLoc);
        virtual boost::optional<IndexKeyEntry> seek(const IndexSeekPoint& seekPoint,
                                                    RequestedInfo parts = kKeyAndLoc);
        virtual void save();
        virtual void restore();
        virtual void detachFromOperationContext();
        virtual void reattachToOperationContext(OperationContext* opCtx);

    private:
        // This is a helper function for seek.
        boost::optional<IndexKeyEntry> seekAfterProcessing(BSONObj finalKey, bool inclusive);
        OperationContext* opCtx;
        // This is the "working copy" of the master "branch" in the git analogy.
        StringStore* workingCopy;
        // These store the end positions.
        boost::optional<StringStore::iterator> endPos;
        boost::optional<StringStore::reverse_iterator> endPosReverse;
        // This stores whether or not the endPosition has been set.
        bool endPosValid;
        // This means if the cursor is a forward or reverse cursor.
        bool _forward;
        // This means whether the cursor has reached the last EOF (with regard to this index).
        bool atEOF;
        // This means whether or not the last move was restore.
        bool lastMoveWasRestore;
        // This is the keystring for the saved location.
        std::string saveKey;
        // These are the same as before.
        std::string _prefix;
        std::string _postfix;
        // These two store the iterator, which is the data structure for cursors. The one we use
        // depends on _forward.
        StringStore::iterator forwardIt;
        StringStore::reverse_iterator reverseIt;
        // This is the ordering for the key's values for multi-field keys.
        Ordering _order;
        // This is the keystring representation of the prefix/postfix.
        std::string _postfixBSON;
        std::string _prefixBSON;
        // This stores whether or not the end position is inclusive for restore.
        bool endPosIncl;
        // This stores the key for the end position.
        boost::optional<BSONObj> endPosKey;
        // This stores whether or not the index is unique.
        bool isUnique;
    };

private:
    const Ordering _order;
    // These two are the same as before.
    std::string _prefix;
    std::string _postfix;
    // These are the keystring representations of the _prefix and the _postfix.
    std::string _prefixBSON;
    std::string _postfixBSON;
    // This stores whether or not the end position is inclusive.
    bool isUnique;
    // This stores whethert or not dups are allowed.
    bool dupsAllowed;
};
}  // namespace biggie
}  // namespace mongo
