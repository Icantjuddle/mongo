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

Store::Iterator::Iterator(std::map<Key, Store::Mapped>::iterator iter) : iter(iter) {}

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

Store::ConstIterator::ConstIterator(std::map<Key, Store::Mapped>::const_iterator iter)
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

Store::Size Store::size() const {
    return map.size();
}

Store::Size Store::dataSize() const {
    Store::Size s = Store::Size(0);
    for (auto iter = this->cbegin(); iter != this->cend(); ++iter) {
        s += iter->second.size();
    }

    return s;
}

Store::Size Store::totalSize() const {
    Store::Size s = Store::Size(0);

    for (auto iter = this->cbegin(); iter != this->cend(); ++iter) {
        s += iter->second.size();
        s += iter->first.second;
        s += sizeof(uint8_t*);
        s += sizeof(std::pair<const Key, Mapped>);
        s += sizeof(std::pair<uint8_t*, size_t>);
        s += sizeof(std::string);
    }

    return s;
}

void Store::clear() noexcept {
    map.clear();
}

std::pair<Store::Iterator, bool> Store::insert(Value&& value) {
    auto res = this->map.insert(value);
    Store::Iterator iter = Store::Iterator(res.first);
    return std::pair<Store::Iterator, bool>(iter, res.second);
}

Store::Size Store::erase(const Key& key) {
    return map.erase(key);
}

Store& Store::merge3(const Store& base, const Store& other) {
    Store* store = new Store();

    for (Store::ConstIterator iter = this->cbegin(); iter != this->cend(); ++iter) {
        Store::ConstIterator baseIter = base.cfind(iter->first);
        Store::ConstIterator otherIter = other.cfind(iter->first);

        if (baseIter != base.cend() && otherIter != other.cend()) {
            // Conflicting modifications
            if (iter->second != baseIter->second && otherIter->second != baseIter->second) {
                throw merge_conflict_exception();
            }

            // Non conflicting modifications and no modifications
            if (iter->second != baseIter->second) {
                store->insert(std::pair<const Key, Mapped>(*iter));
            } else {
                store->insert(std::pair<const Key, Mapped>(*otherIter));
            }
        } else if (baseIter != base.cend() && otherIter == other.cend()) {
            // Conflicting modification and deletion
            if (iter->second != baseIter->second) {
                throw merge_conflict_exception();
            }
        } else if (baseIter == base.cend()) {
            // Conflicting insertions
            if (otherIter != other.cend() && iter->second != otherIter->second) {
                throw merge_conflict_exception();
            }
            // Insertions
            store->insert(std::pair<const Key, Mapped>(*iter));
        }
    }

    for (Store::ConstIterator iter = other.cbegin(); iter != other.cend(); ++iter) {
        Store::ConstIterator baseIter = base.cfind(iter->first);
        Store::ConstIterator thisIter = this->cfind(iter->first);

        if (baseIter == base.cend()) {
            // Insertion
            store->insert(std::pair<const Key, Mapped>(*iter));
        } else if (thisIter == this->cend() && iter->second != baseIter->second) {
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

Store::ConstIterator Store::cbegin() const noexcept {
    return Store::ConstIterator(this->map.cbegin());
}

Store::ConstIterator Store::cend() const noexcept {
    return Store::ConstIterator(this->map.cend());
}

Store::ConstIterator Store::cfind(const Key& key) const noexcept {
    return Store::ConstIterator(this->map.find(key));
}

Store& Store::getPrefix(const Key&& prefix) {
    Store* store = new Store();

    for (auto iter = this->cbegin(); iter != this->cend(); ++iter) {
        if (iter->first.second >= prefix.second) {
            if (memcmp(iter->first.first, prefix.first, prefix.second) == 0) {
                store->insert(std::pair<const Key, Mapped>(*iter));
            }
        }
    }
    return *store;
}

Store::Size Store::rangeScan(const Key& key1, const Key& key2) {
    Store::Size s = Store::Size(0);
    keyCmp cmp;
    for (auto iter = this->cbegin(); iter != this->cend(); ++iter) {
        if (!cmp.operator()(iter->first, key1) && cmp.operator()(iter->first, key2)) {
            s++;
        }
    }
    return s;
}

bool Store::keyCmp::operator()(const Key& a, const Key& b) const {
    if (a.second == b.second) {
        return memcmp(a.first, b.first, a.second) < 0;
    }

    int shorter;
    if (a.second < b.second) {
        shorter = a.second;
    } else {
        shorter = b.second;
    }

    int res = memcmp(a.first, b.first, a.second);
    if (res == 0) {
        return a.second < b.second;
    } else {
        return res < 0;
    }
}
}  // namespace mongo
