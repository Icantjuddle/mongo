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

#include <array>
#include <boost/optional.hpp>
#include <exception>
#include <string>
#include <stack>
#include <iostream>
#include <memory>

namespace mongo {
namespace biggie {

class merge_conflict_exception : std::exception {
    virtual const char* what() const noexcept {
        return "conflicting changes prevent successful merge";
    }
};

template <class Key, class T>
class RadixStore {
    class Node;

public:
    using mapped_type = T;
    using value_type = std::pair<const Key, mapped_type>;
    using allocator_type = std::allocator<value_type>;
    using pointer = typename std::allocator_traits<allocator_type>::pointer;
    using const_pointer = typename std::allocator_traits<allocator_type>::const_pointer;
    using size_type = std::size_t;

    template <class pointer_type, class reference_type>
    class radix_iterator {
        friend class RadixStore;

        const std::shared_ptr<Node> root;
        Node* current;
        bool reverse;

        //constructors
        radix_iterator() : root(nullptr), current(nullptr), reverse(false) {}

        radix_iterator(const std::shared_ptr<Node>& root, Node* current)
            : root(root), current(current), reverse(false){};

        radix_iterator(const std::shared_ptr<Node>& root, Node* current, bool rev)
            : root(root), current(current), reverse(rev){};

        void findNext() {
            // if current is a nullptr we've finished iterating
            if (current == nullptr)
                return;

            // if our current node is not a leaf, we can
            // continue moving down and left in our tree
            if (!current->isLeaf()) {
                traverseLeftSubtree();
                return;
            }

            Key key = current->data->first;
            std::stack<Node*> context;
            Node* cur = root.get();
            context.push(cur);
            for (char& c : key) {
                cur = cur->children[c].get();
                context.push(cur);
            }

            // need to go back up
            Node* it = context.top();
            context.pop();
            char oldKey;
            current = nullptr;
            while (!context.empty()) {
                oldKey = it->trieKey;
                it = context.top();
                context.pop();
                // explore the next branches
                for (int i = oldKey + 1; i < 256; i++) {
                    if (it->children[i] != nullptr) {
                        current = it->children[i].get();
                        traverseLeftSubtree();
                        return;
                    }
                }
            }
            return;
        }

        void findNextReverse() {
            // if current is a nullptr we've finished iterating
            if (current == nullptr)
                return;

            Key key = current->data->first;
            std::stack<Node*> context;
            Node* cur = root.get();
            context.push(cur);
            for (char& c : key) {
                cur = cur->children[c].get();
                context.push(cur);
            }

            // need to go back up
            Node* it = context.top();
            context.pop();
            char oldKey;
            current = nullptr;
            while (!context.empty()) {
                oldKey = it->trieKey;
                it = context.top();
                context.pop();
                // explore the next branches
                for (int i = oldKey - 1; i >= 0; i--) {
                    if (it->children[i] != nullptr) {
                        current = it->children[i].get();
                        traverseRightSubtree();
                        return;
                    }
                }

                if (it->data != boost::none) {
                    current = it;
                    return;
                }
            }
        }

        void traverseLeftSubtree() {
            //want to do at least one iteration
            do {
                for (int i = 0; i < 256; i++) {
                    if (current->children[i] != nullptr) {
                        current = current->children[i].get();
                        break;
                    }
                }
            } while (current->data == boost::none);
        }

        void traverseRightSubtree() {
            while (!current->isLeaf()) {
                for (int i = 255; i >= 0; i--) {
                    if (current->children[i] != nullptr) {
                        current = current->children[i].get();
                        break;
                    }
                }
            }
        }

    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = RadixStore::value_type;
        using difference_type = std::ptrdiff_t;
        using pointer = pointer_type;
        using reference = reference_type;

        ~radix_iterator() = default;

        radix_iterator& operator++() {
            if (reverse)
                findNextReverse();
            else
                findNext();

            return *this;
        }

        radix_iterator operator++(int) {
            radix_iterator old = *this;
            if (reverse)
                findNextReverse();
            else
                findNext();

            return old;
        }

        bool operator==(const radix_iterator& other) const {
            return this->current == other.current;
        }

        bool operator!=(const radix_iterator& other) const {
            return this->current != other.current;
        }

        reference operator*() const {
            return *(current->data);
        }

        const_pointer operator->() {
            return &*(current->data);
        }
    };

    using iterator = radix_iterator<pointer, value_type&>;
    using const_iterator = radix_iterator<const_pointer, const value_type&>;

    // Constructors
    RadixStore(const RadixStore& other) {
        root = other.root;
        numElems = other.numElems;
        sizeElems = other.sizeElems;
        std::stack<std::shared_ptr<Node>> context;
        std::stack<std::shared_ptr<Node>> otherContext;
        otherContext.push(other.root);
        context.push(root);
        while (!context.empty()) {
            std::shared_ptr<Node> cursor = context.top();
            context.pop();
            std::shared_ptr<Node> otherCursor = otherContext.top();
            otherContext.pop();

            for (int i=0; i<256; i++) {
                cursor->children[i] = otherCursor->children[i];
                if (cursor->children[i] != nullptr) {
                    context.push(cursor->children[i]);
                    otherContext.push(otherCursor->children[i]);
                }
            }
        }
    }

    RadixStore() {
        root = std::make_shared<RadixStore::Node>('\0');
        numElems = 0;
        sizeElems = 0;
    }

    ~RadixStore() = default;

    // Equality
    bool operator==(const RadixStore& other) const {
        RadixStore::const_iterator iter = this->begin();
        RadixStore::const_iterator other_iter = other.begin();

        while (iter != this->end()) {
            if (*iter != *other_iter) {
                return false;
            }

            iter++;
            other_iter++;
        }

        if (other_iter != other.end())
            return false;

        return true;
    }

    // Capacity
    bool empty() const {
        return numElems == 0;
    }

    size_type size() const {
        return numElems;
    }

    size_type dataSize() const {
        return sizeElems;
    }

    // Modifiers
    void clear() noexcept {
        root = std::make_shared<Node>('\0');
        numElems = 0;
        sizeElems = 0;
    }

    std::pair<const_iterator, bool> insert(value_type&& value) {
        Key key = value.first;

        // empty keys are not allowed
        if (value.first.size() == 0)
            return std::make_pair(RadixStore::end(), false);

        // if key exists, insert fails
        auto item = RadixStore::find(key);
        if (item != RadixStore::end())
            return std::make_pair(item, false);

        if (root.use_count() > 1) {
            auto old = root;
            root = std::make_shared<Node>('\0');
            auto cur = root;
            for (char& c : key) {
                if (old != nullptr) {
                    for (int i = 0; i < 256; i++) {
                        cur->children[i] = old->children[i];
                    }
                    old = old->children[c];
                }

                cur->children[c] = std::make_shared<Node>(c);
                cur = cur->children[c];
            }
            cur->data.emplace(value.first, value.second);
            RadixStore::const_iterator it(root, cur.get());
            numElems++;
            sizeElems += value.second.size();
            return std::pair<RadixStore::const_iterator, bool>(it, true);
        } else {
            return RadixStore::insertHelper(std::move(value));
        }
    }

    std::pair<const_iterator, bool> update(value_type&& value) {
        Key key = value.first;

        //if item does not exist - cannot update
        auto item = RadixStore::find(key);
        if (item == RadixStore::end()) 
            return std::make_pair(item, false);

        if (root.use_count() > 1) {
            auto old = root;
            root = std::make_shared<Node>('\0');
            auto cur = root;
            for (char& c : key) {
                for (int i=0; i<256; i++) {
                    cur->children[i] = old->children[i];
                }
                cur->children[c] = std::make_shared<Node>(c);
                cur = cur->children[c];
                old = old->children[c];
            }
            sizeElems -= old->data->second.size();
            sizeElems += value.second.size();

            cur->data.emplace(value.first, value.second);

            RadixStore::const_iterator it(root, cur.get());
            return std::pair<RadixStore::const_iterator, bool>(it, true);
        } else {
            auto cur = root;
            for (char& c : key) {
                if (cur->children[c] != nullptr) {
                    cur = cur->children[c];
                } else {
                    cur->children[c] = std::make_shared<Node>(c);
                    cur = cur->children[c];
                }
            }

            sizeElems -= cur->data->second.size();
            sizeElems += value.second.size();
            cur->data.emplace(value.first, value.second);
            RadixStore::const_iterator it = RadixStore::const_iterator(root, cur.get());

            return std::pair<RadixStore::const_iterator, bool>(it, true);
        }
    }

    size_type erase(const Key& key) {
        std::stack<Node*> context;
        Node* cur = root.get();
        for (char c : key) {
            cur = cur->children[c].get();
            if (cur == nullptr)
                return false;
            context.push(cur);
        }

        cur = context.top();
        context.pop();
        numElems--;
        sizeElems -= cur->data->second.size();
        cur->data = boost::none;

        char tKey;
        while (!context.empty() && cur->isLeaf() && cur->data != boost::none) {
            tKey = cur->trieKey;
            cur = context.top();
            cur->children[tKey] = nullptr;
            context.pop();
        }

        return true;
    }

    // Returns a Store that has all changes from both 'this' and 'other' compared to base.
    // Throws merge_conflict_exception if there are merge conflicts.
    RadixStore merge3(const RadixStore& base, const RadixStore& other) const {
        RadixStore store;

        // Merges all differences between this and base, along with modifications from other.
        RadixStore::const_iterator iter = this->begin();
        for (; iter != this->end(); iter++) {
            const value_type val = *iter;
            RadixStore::const_iterator baseIter = base.find(val.first);
            RadixStore::const_iterator otherIter = other.find(val.first);

            if (baseIter != base.end() && otherIter != other.end()) {
                if (val.second != baseIter->second && otherIter->second != baseIter->second) {
                    // Throws exception if there are conflicting modifications.
                    throw merge_conflict_exception();
                }

                if (val.second != baseIter->second) {
                    // Merges non-conflicting insertions from this.
                    store.insert(RadixStore::value_type(val));
                } else {
                    // Merges non-conflicting modifications from other or no modifications.
                    store.insert(RadixStore::value_type(*otherIter));
                }
            } else if (baseIter != base.end() && otherIter == other.end()) {
                if (val.second != baseIter->second) {
                    // Throws exception if modifications from this conflict with deletions from
                    // other.
                    throw merge_conflict_exception();
                }
            } else if (baseIter == base.end()) {
                if (otherIter != other.end()) {
                    // Throws exception if insertions from this conflict with insertions from other.
                    throw merge_conflict_exception();
                }

                // Merges insertions from this.
                store.insert(RadixStore::value_type(val));
            }
        }

        // Merges insertions and deletions from other.
        RadixStore::const_iterator other_iter = other.begin();
        for (; other_iter != other.end(); other_iter++) {
            const value_type otherVal = *other_iter;
            RadixStore::const_iterator baseIter = base.find(otherVal.first);
            RadixStore::const_iterator thisIter = this->find(otherVal.first);

            if (baseIter == base.end()) {
                // Merges insertions from other.
                store.insert(RadixStore::value_type(otherVal));
            } else if (thisIter == this->end() && otherVal.second != baseIter->second) {
                // Throws exception if modifications from this conflict with deletions from other.
                throw merge_conflict_exception();
            }
        }

        return store;
    }
    
    // iterators
    const_iterator begin() const noexcept {
        if (numElems == 0)
            return RadixStore::end();

        auto cur = root;
        while (cur->data == boost::none) {
            for (int i = 0; i < 256; i++) {
                if (cur->children[i] != nullptr) {
                    cur = cur->children[i];
                    break;
                }
            }
        }
        return RadixStore::const_iterator(root, cur.get());
    }

    const_iterator rbegin() const noexcept {
        if (numElems == 0)
            return RadixStore::rend();

        auto cur = root;
        while (!cur->isLeaf()) {
            for (int i = 255; i >= 0; i--) {
                if (cur->children[i] != nullptr) {
                    cur = cur->children[i];
                    break;
                }
            }
        }
        return RadixStore::const_iterator(root, cur.get(), true);
    }

    const_iterator end() const noexcept {
        return RadixStore::const_iterator();
    }

    const_iterator rend() const noexcept {
        return RadixStore::const_iterator();
    }

    //iterator begin() const noexcept;
    //iterator rbegin() const noexcept;
    //iterator end() const noexcept;
    //iterator rend() const noexcept;

    //iterator find(const Key& key);
    const_iterator find(const Key& key) const {
        RadixStore::const_iterator it = RadixStore::end();

        auto cur = root;
        for (char c : key) {
            if (cur->children[c] != nullptr)
                cur = cur->children[c];
            else
                return it;
        }
        
        if (cur->data != boost::none)
            return RadixStore::const_iterator(root, cur.get());
        else
            return it;
    }

    const_iterator rlower_bound(const Key& key) const {
        const_iterator it = find(key);
        it.reverse = true;
        return it;
    } 

    const_iterator rupper_bound(const Key& key) const {
        const_iterator it = find(key);
        it.reverse = true;
        if (it == rend())
            return it;

        it++;
        return it;
    } 

    const_iterator lower_bound(const Key& key) const {
        return find(key);
    } 

    const_iterator upper_bound(const Key& key) const {
        const_iterator it = find(key);
        if (it == end())
            return it;

        it++;
        return it;
    } 

    typename RadixStore::iterator::difference_type distance(iterator iter1, iterator iter2) {
        return std::distance(iter1, iter2);
    }

    typename RadixStore::iterator::difference_type distance(const_iterator iter1, const_iterator iter2) {
        return std::distance(iter1, iter2);
    }
    
private:
    class Node {
    private:
        friend class RadixStore;

    public:
        char trieKey;
        boost::optional<value_type> data;
        std::array<std::shared_ptr<Node>, 256> children;

        Node(char key) : trieKey(key) {
            children.fill(nullptr);
        }

        bool isLeaf() {
            for (int i = 0; i < 256; i++) {
                if (children[i] != nullptr)
                    return false;
            }
            return true;
        }
    };

    std::pair<const_iterator, bool> insertHelper(value_type&& value) {
        Key key = value.first;

        auto cur = root;
        for (char& c : key) {
            if (cur->children[c] != nullptr) {
                cur = cur->children[c];
            } else {
                cur->children[c] = std::make_shared<Node>(c);
                cur = cur->children[c];
            }
        }

        cur->data.emplace(value.first, value.second);
        RadixStore::const_iterator it = RadixStore::const_iterator(root, cur.get());
        numElems++;
        sizeElems += value.second.size();
        return std::pair<RadixStore::const_iterator, bool>(it, true);
    }

    std::shared_ptr<Node> root;
    size_type numElems;
    size_type sizeElems;
};

}  // namespace biggie
}  // namespace mongo
