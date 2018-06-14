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

#include "mongo/platform/basic.h"

#include "mongo/db/storage/biggie/biggie_record_store.h"
#include "mongo/db/storage/biggie/biggie_store.h"

#include "mongo/base/init.h"
#include "mongo/db/storage/record_store_test_harness.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace {

class BiggieHarnessHelper final : public RecordStoreHarnessHelper {
public:
    BiggieHarnessHelper() {}

    virtual std::unique_ptr<RecordStore> newNonCappedRecordStore() {
        return newNonCappedRecordStore("a.b");
    }

    virtual std::unique_ptr<RecordStore> newNonCappedRecordStore(const std::string& ns) {
        return stdx::make_unique<BiggieRecordStore>(ns, data, false, 0, 0, nullptr);
    }

    virtual std::unique_ptr<RecordStore> newCappedRecordStore(int64_t cappedSizeBytes,
                                                              int64_t cappedMaxDocs) {
        return newCappedRecordStore("a.b", cappedSizeBytes, cappedMaxDocs);
    }

    virtual std::unique_ptr<RecordStore> newCappedRecordStore(const std::string& ns,
                                                              int64_t cappedSizeBytes,
                                                              int64_t cappedMaxDocs) final {
        return std::make_unique<BiggieRecordStore>(
            ns, data, true, cappedSizeBytes, cappedMaxDocs, nullptr);
    }

    std::unique_ptr<RecoveryUnit> newRecoveryUnit() final {
        return stdx::make_unique<RecoveryUnitNoop>();
    }

    bool supportsDocLocking() final {
        return false;
    }

    std::shared_ptr<BiggieStore> data;
};

std::unique_ptr<HarnessHelper> makeHarnessHelper() {
    return stdx::make_unique<BiggieHarnessHelper>();
}

MONGO_INITIALIZER(RegisterHarnessFactory)(InitializerContext* const) {
    mongo::registerHarnessHelperFactory(makeHarnessHelper);
    return Status::OK();
}
}  // namespace
}  // namespace mongo
