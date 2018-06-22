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
= */
#pragma once

#include "mongo/db/storage/biggie/store.h"
#include <atomic>

namespace mongo {
class BiggieStore : public Store {
private:
    std::map<Key, Mapped>* map = new std::map<Key, Mapped>();
    std::atomic<std::int64_t> highest_record_id;

public:
    bool operator==(const Store& other) const;

    bool empty() const;
    Store::Size size() const;

    void clear() noexcept;
    std::pair<Store::Iterator, bool> insert(Value&& value);
    Store::Size erase(const Key& key);

    Store& merge3(const Store& base, const Store& other);

    Iterator begin() noexcept;
    Iterator end() noexcept;
    Iterator find(const Key& key) noexcept;

    /**
     * @brief Gets the next (garunteed) unique record id.
     *
     * @return int64_t The next unusued record id.
     */
    inline int64_t nextRecordId() {
        return highest_record_id.fetch_add(1);
    }
};
}
