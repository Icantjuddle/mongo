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
using Key = std::string;

class Store {
public:
    using mapped_type = T;
    using value_type = std::pair<const Key, mapped_type>;
    using allocator_type = std::allocator<value_type>;
    using pointer = std::allocator_traits<allocator_type>::pointer;
    using const_pointer = std::allocator_traits<allocator_type>::const_pointer;
    using size_type = std::size_t;

    class Iterator {
    private:
        friend class Store;
        std::map<Key, mapped_type>::iterator iter;

    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = value_type;
        using difference_type = std::ptrdiff_t;
        using pointer = Store::pointer;
        using reference = value_type&;

        Iterator(std::map<Key, mapped_type>::iterator iter);
        Iterator& operator++();
        bool operator==(const Iterator& other) const;
        bool operator!=(const Iterator& other) const;
        reference operator*() const;
        pointer operator->();
    };

    class ConstIterator {
    private:
        friend class Store;
        std::map<Key, mapped_type>::const_iterator iter;

    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = const value_type;
        using difference_type = std::ptrdiff_t;
        using pointer = Store::const_pointer;
        using reference = const value_type&;

        ConstIterator(std::map<Key, mapped_type>::const_iterator iter);
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
    size_type size() const;      // Number of nodes
    size_type dataSize() const;  // Size of mapped data

    // Modifiers
    void clear() noexcept;
    std::pair<Iterator, bool> insert(value_type&& value);
    size_type erase(const Key& key);

    // Returns a Store that has all changes from both 'this' and 'other' compared to base.
    // Throws merge_conflict_exception if there are merge conflicts.
    Store& merge3(const Store& base, const Store& other) const;

    // Iterators
    Iterator begin() noexcept;
    Iterator end() noexcept;
    Iterator find(const Key& key) noexcept;

    ConstIterator begin() const noexcept;
    ConstIterator end() const noexcept;
    ConstIterator find(const Key& key) const noexcept;

    // std::distance
    Store::Iterator::difference_type distance(Iterator iter1, Iterator iter2);

private:
    std::map<Key, mapped_type> map = std::map<Key, mapped_type>();
};
}
