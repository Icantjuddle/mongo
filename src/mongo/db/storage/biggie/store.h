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

#pragma once

#include <atomic>
#include <cstring>
#include <exception>
#include <map>
#include <memory>
#include <string>
#include <utility>

namespace mongo {

class merge_conflict_exception : std::exception {
    virtual const char* what() const noexcept {
        return "conflicting changes prevent successful merge";
    }
};

using T = std::string;
using Key = std::pair<uint8_t*, size_t>;

class Store {
public:
    using Mapped = T;
    using Value = std::pair<const Key, Mapped>;
    using Allocator = std::allocator<Value>;
    using Pointer = std::allocator_traits<Allocator>::pointer;
    using ConstPointer = std::allocator_traits<Allocator>::const_pointer;
    using Size = std::size_t;

    class Iterator {
    private:
        friend class Store;
        std::map<Key, Mapped>::iterator iter;

    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = Value;
        using difference_type = std::ptrdiff_t;
        using pointer = Store::Pointer;
        using reference = Value&;

        Iterator(std::map<Key, Mapped>::iterator iter);
        Iterator& operator++();
        bool operator==(const Iterator& other) const;
        bool operator!=(const Iterator& other) const;
        reference operator*() const;
        pointer operator->();
    };

    class ConstIterator {
    private:
        friend class Store;
        std::map<Key, Mapped>::const_iterator iter;

    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = const Value;
        using difference_type = std::ptrdiff_t;
        using pointer = Store::ConstPointer;
        using reference = const Value&;

        ConstIterator(std::map<Key, Mapped>::const_iterator iter);
        ConstIterator& operator++();
        bool operator==(const ConstIterator& other) const;
        bool operator!=(const ConstIterator& other) const;
        reference operator*() const;
        pointer operator->();
    };

    // Constructors
    Store(const Store& other) = default;
    Store() = default;

    ~Store() = default;

    // Equality
    bool operator==(const Store& other) const;

    // Capacity
    bool empty() const;
    Size size() const;
    Size dataSize() const;
    Size totalSize() const;

    // Modifiers
    void clear() noexcept;
    std::pair<Iterator, bool> insert(Value&& value);
    Size erase(const Key& key);

    // Returns a Store that has all changes from both 'this' and 'other' compared to base.
    // Throws merge_conflict_exception if there are merge conflicts.
    Store& merge3(const Store& base, const Store& other);

    // Iterators
    Iterator begin() noexcept;
    Iterator end() noexcept;
    Iterator find(const Key& key) noexcept;

    ConstIterator cbegin() const noexcept;
    ConstIterator cend() const noexcept;
    ConstIterator cfind(const Key& key) const noexcept;

    // Get all nodes whose prefix matches prefix
    Store& getPrefix(const Key&& prefix);

    // Get the number of nodes that fall between [key1, key2)
    Size rangeScan(const Key& key1, const Key& key2);

    /**
     * @brief Gets the next (guaranteed) unique record id.
     *
     * @return int64_t The next unusued record id.
     */
    inline int64_t nextRecordId() {
        return highest_record_id.fetch_add(1);
    }

    struct keyCmp {
        bool operator()(const Key& a, const Key& b) const;
    };

private:
    std::map<Key, Mapped, keyCmp> map = std::map<Key, Mapped, keyCmp>();
    std::atomic<std::int64_t> highest_record_id;
};
}
