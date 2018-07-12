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
private:
    const Ordering _order;
    std::string _prefix;
    std::string _postfix;
    std::string _prefixBSON;
    std::string _postfixBSON;
    bool isUnique;
    bool dupsAllowed;

public:
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
    // default is a forward Cursor
    class Cursor final : public ::mongo::SortedDataInterface::Cursor {
    private:
        OperationContext* opCtx;
        StringStore* workingCopy;  // should not be nullptr
        boost::optional<StringStore::iterator> endPos;
        boost::optional<StringStore::reverse_iterator> endPosReverse;
        bool endPosValid;
        bool _forward;
        bool atEOF;
        bool lastMoveWasRestore;
        std::string saveKey;
        std::string _prefix;
        std::string _postfix;
        StringStore::iterator forwardIt;
        StringStore::reverse_iterator reverseIt;
        Ordering _order;
        std::string _postfixBSON;
        std::string _prefixBSON;
        bool endPosIncl;
        boost::optional<BSONObj> endPosKey;
        bool isUnique;
        boost::optional<IndexKeyEntry> seekAfterProcessing(BSONObj finalKey, bool inclusive);

    public:
        Cursor(OperationContext* opCtx,
               bool isForward,
               std::string _prefix,
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
    };
};
}  // namespace biggie
}  // namespace mongo
