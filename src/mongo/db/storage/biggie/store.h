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

#include <utility>
#include <exception>
#include <map>
#include <string>
#include <memory>

#pragma once

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
        using Size = std::size_t;

        class Iterator
        : public std::iterator<std::forward_iterator_tag, Value, std::ptrdiff_t, Pointer, Value&> {
            friend class Store;
        };

        // Constructors
        Store(const Store& other) = default;

        // Capacity

        virtual bool empty() const = 0;
        virtual Size size() const = 0;

        // Modifiers

        virtual void clear() noexcept = 0;
        //virtual std::pair<Iterator, bool> insert(Value&& value) = 0;
        virtual bool insert(Value&& value) = 0;
        virtual Size erase(const Key& key) = 0;

        // Returns a Store that has all changes from both 'this' and 'other' compared to base.
        // Throws merge_conflict_exception if there are merge conflicts.
        virtual Store& merge3(const Store& base, const Store& other) = 0;

        // Iterators

        //virtual Iterator begin() noexcept = 0;
        //virtual Iterator end() noexcept = 0;
        //virtual Iterator find(const Key& key) noexcept = 0;

};
}
