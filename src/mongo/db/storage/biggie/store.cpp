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

#include "mongo/platform/basic.h"

#include "mongo/db/storage/biggie/store.h"

namespace mongo {

Store::Iterator::Iterator(std::map<Key, Store::mapped_type>::iterator iter) : iter(iter) {}

Store::Iterator& Store::Iterator::operator++() {
    ++this->iter;
    return *this;
}

bool Store::Iterator::operator==(const Store::Iterator& other) const {
    return this->iter == other.iter;
}

bool Store::Iterator::operator!=(const Store::Iterator& other) const {
    return this->iter != other.iter;
}

Store::Iterator::reference Store::Iterator::operator*() const {
    return *this->iter;
}

Store::Iterator::pointer Store::Iterator::operator->() {
    return &(*this->iter);
}

Store::ConstIterator::ConstIterator(std::map<Key, Store::mapped_type>::const_iterator iter)
    : iter(iter) {}

Store::ConstIterator& Store::ConstIterator::operator++() {
    ++this->iter;
    return *this;
}

bool Store::ConstIterator::operator==(const Store::ConstIterator& other) const {
    return this->iter == other.iter;
}

bool Store::ConstIterator::operator!=(const Store::ConstIterator& other) const {
    return this->iter != other.iter;
}

Store::ConstIterator::reference Store::ConstIterator::operator*() const {
    return *this->iter;
}

Store::ConstIterator::pointer Store::ConstIterator::operator->() {
    return &(*this->iter);
}

bool Store::operator==(const Store& other) const {
    return this->map == other.map;
}

bool Store::empty() const {
    return map.empty();
}

Store::size_type Store::size() const {
    return map.size();
}

Store::size_type Store::dataSize() const {
    Store::size_type s = Store::size_type(0);
    for (const std::pair<const Key, mapped_type> val : *this) {
        s += val.second.size();
    }

    return s;
}

void Store::clear() noexcept {
    map.clear();
}

std::pair<Store::Iterator, bool> Store::insert(value_type&& value) {
    auto res = this->map.insert(value);
    Store::Iterator iter = Store::Iterator(res.first);
    return std::pair<Store::Iterator, bool>(iter, res.second);
}

Store::size_type Store::erase(const Key& key) {
    return map.erase(key);
}

Store& Store::merge3(const Store& base, const Store& other) const {
    Store* store = new Store();

    for (const std::pair<const Key, mapped_type> val : *this) {
        Store::ConstIterator baseIter = base.find(val.first);
        Store::ConstIterator otherIter = other.find(val.first);

        if (baseIter != base.end() && otherIter != other.end()) {
            // Conflicting modifications
            if (val.second != baseIter->second && otherIter->second != baseIter->second) {
                throw merge_conflict_exception();
            }

            // Non conflicting modifications and no modifications
            if (val.second != baseIter->second) {
                store->insert(std::pair<const Key, mapped_type>(val));
            } else {
                store->insert(std::pair<const Key, mapped_type>(*otherIter));
            }
        } else if (baseIter != base.end() && otherIter == other.end()) {
            // Conflicting modification and deletion
            if (val.second != baseIter->second) {
                throw merge_conflict_exception();
            }
        } else if (baseIter == base.end()) {
            // Conflicting insertions
            if (otherIter != other.end() && val.second != otherIter->second) {
                throw merge_conflict_exception();
            }
            // Insertions
            store->insert(std::pair<const Key, mapped_type>(val));
        }
    }

    for (const std::pair<const Key, mapped_type> val : other) {
        Store::ConstIterator baseIter = base.find(val.first);
        Store::ConstIterator thisIter = this->find(val.first);

        if (baseIter == base.end()) {
            // Insertion
            store->insert(std::pair<const Key, mapped_type>(val));
        } else if (thisIter == this->end() && val.second != baseIter->second) {
            // Conflicting modification and deletion
            throw merge_conflict_exception();
        }
    }

    return *store;
}

Store::Iterator Store::begin() noexcept {
    return Store::Iterator(this->map.begin());
}

Store::Iterator Store::end() noexcept {
    return Store::Iterator(this->map.end());
}

Store::Iterator Store::find(const Key& key) noexcept {
    return Store::Iterator(this->map.find(key));
}

Store::ConstIterator Store::begin() const noexcept {
    return Store::ConstIterator(this->map.begin());
}

Store::ConstIterator Store::end() const noexcept {
    return Store::ConstIterator(this->map.end());
}

Store::ConstIterator Store::find(const Key& key) const noexcept {
    return Store::ConstIterator(this->map.find(key));
}

Store::Iterator::difference_type Store::distance(Store::Iterator iter1, Store::Iterator iter2) {
    return std::distance(iter1.iter, iter2.iter);
}
}  // namespace mongo
