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

#include "mongo/db/storage/sorted_data_interface.h"

#pragma once

namespace mongo {
class BiggieSortedImpl : public SortedDataInterface {
public:
    virtual SortedDataBuilderInterface* getBulkBuilder(OperationContext *opCtx, bool dupsAllowed);
    virtual Status insert(OperationContext *opCtx, const BSONObj &key, const RecordId &loc, bool dupsAllowed);
    virtual void unindex(OperationContext *opCtx, const BSONObj &key, const RecordId &loc, bool dupsAllowed);
    virtual Status dupKeyCheck(OperationContext *opCtx, const BSONObj &key, const RecordId &loc);
    virtual void fullValidate(OperationContext *opCtx, long long *numKeysOut, ValidateResults *fullResults) const;
    virtual bool appendCustomStats(OperationContext *opCtx, BSONObjBuilder *output, double scale) const;
    virtual long long getSpaceUsedBytes(OperationContext *opCtx) const;
    virtual bool isEmpty (OperationContext *opCtx);
    // what cursor is this
    // this is not the right cursor I think
    virtual std::unique_ptr<BiggieSortedImpl::Cursor>newCursor(OperationContext *opCtx, bool isForward=true) const;
    virtual Status initAsEmpty(OperationContext *opCtx);
};
}