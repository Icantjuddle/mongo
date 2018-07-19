/**
 *    Copyright 2017 MongoDB Inc.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/db/storage/biggie/radix_store.h"
#include "mongo/platform/basic.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/log.h"
#include <iostream>

namespace mongo {
namespace biggie {
namespace {

using StringStore = RadixStore<std::string, std::string>;
using value_type = StringStore::value_type;

class RadixStoreTest : public unittest::Test {
protected:
    StringStore thisStore;
    StringStore otherStore;
    StringStore baseStore;
    StringStore expected;
};

TEST_F(RadixStoreTest, InsertTest) {
    value_type value1 = std::make_pair("1", "foo");
    std::pair<StringStore::const_iterator, bool> res = thisStore.insert(value_type(value1));
    ASSERT_TRUE(res.second);
    ASSERT_TRUE(*res.first == value1);
}

TEST_F(RadixStoreTest, InsertTest2) {
    value_type value1 = std::make_pair("food", "1");
    value_type value2 = std::make_pair("foo", "2");
    value_type value3 = std::make_pair("bar", "2");

    thisStore.insert(value_type(value1));
    std::pair<StringStore::const_iterator, bool> res = thisStore.insert(value_type(value2));

    ASSERT_EQ(thisStore.size(), StringStore::size_type(2));
    ASSERT_TRUE(res.second);
    ASSERT_TRUE(*res.first == value2);

    std::pair<StringStore::const_iterator, bool> res2 = thisStore.insert(value_type(value3));
    ASSERT_EQ(thisStore.size(), StringStore::size_type(3));
    ASSERT_TRUE(res2.second);
}

TEST_F(RadixStoreTest, InsertTest3) {
    value_type value1 = std::make_pair("foo", "1");
    value_type value2 = std::make_pair("fod", "2");
    value_type value3 = std::make_pair("fee", "3");
    value_type value4 = std::make_pair("fed", "5");

    thisStore.insert(value_type(value1));
    thisStore.insert(value_type(value2));
    thisStore.insert(value_type(value3));

    otherStore = thisStore;

    otherStore.insert(value_type(value4));

    StringStore::const_iterator it1 = thisStore.find(value4.first);
    StringStore::const_iterator it2 = otherStore.find(value4.first);

    ASSERT_TRUE(it1 == thisStore.end());
    ASSERT_TRUE(it2 != otherStore.end());

    StringStore::const_iterator check_this = thisStore.begin();
    StringStore::const_iterator check_other = otherStore.begin();

    // Only 'otherStore' should have the 'fed' object, whereas thisStore should point to the 'fee'
    // node
    ASSERT_TRUE(check_other->first == value4.first);
    ASSERT_TRUE(check_this->first == value3.first);
    check_other++;

    // Both should point to the same "fee" node.
    ASSERT_EQUALS(&*check_this, &*check_other);
    check_this++;
    check_other++;

    // Now both should point to the same "fod" node.
    ASSERT_EQUALS(&*check_this, &*check_other);
    check_this++;
    check_other++;

    // Both should point to the same "foo" node.
    ASSERT_EQUALS(&*check_this, &*check_other);
    check_this++;
    check_other++;

    ASSERT_TRUE(check_this == thisStore.end());
    ASSERT_TRUE(check_other == otherStore.end());
}

TEST_F(RadixStoreTest, InsertTest4) {
    value_type value1 = std::make_pair("foo", "1");
    value_type value2 = std::make_pair("fod", "2");
    value_type value3 = std::make_pair("fee", "3");
    value_type value4 = std::make_pair("fed", "4");
    value_type value5 = std::make_pair("food", "5");

    thisStore.insert(value_type(value1));
    thisStore.insert(value_type(value2));
    thisStore.insert(value_type(value3));

    otherStore = thisStore;

    otherStore.insert(value_type(value4));
    otherStore.insert(value_type(value5));

    StringStore::const_iterator check_this = thisStore.begin();
    StringStore::const_iterator check_other = otherStore.begin();

    // Only 'otherStore' should have the 'fed' object, whereas thisStore should point to the 'fee'
    // node
    ASSERT_TRUE(check_other->first == value4.first);
    ASSERT_TRUE(check_this->first == value3.first);
    check_other++;

    // Both should point to the same "fee" node.
    ASSERT_EQUALS(&*check_this, &*check_other);
    check_this++;
    check_other++;

    // Now both should point to the same "fod" node.
    ASSERT_EQUALS(&*check_this, &*check_other);
    check_this++;
    check_other++;

    // Both should point to "foo", but they should be different objects
    ASSERT_TRUE(check_this->first == value1.first);
    ASSERT_TRUE(check_other->first == value1.first);
    ASSERT_TRUE(&*check_this != &*check_other);
    check_this++;
    check_other++;

    ASSERT_TRUE(check_this == thisStore.end());
    ASSERT_TRUE(check_other->first == value5.first);
    check_other++;

    ASSERT_TRUE(check_other == otherStore.end());
}

TEST_F(RadixStoreTest, InsertTest5) {
    value_type value1 = std::make_pair("foo", "1");
    value_type value2 = std::make_pair("fod", "2");
    value_type value3 = std::make_pair("fee", "3");
    value_type value4 = std::make_pair("fed", "4");
    value_type value5 = std::make_pair("feed", "5");

    thisStore.insert(value_type(value1));
    thisStore.insert(value_type(value2));
    thisStore.insert(value_type(value3));

    otherStore = thisStore;

    otherStore.insert(value_type(value4));
    otherStore.insert(value_type(value5));

    StringStore::const_iterator check_this = thisStore.begin();
    StringStore::const_iterator check_other = otherStore.begin();

    // Only 'otherStore' should have the 'fed' object, whereas thisStore should point to the 'fee'
    // node
    ASSERT_TRUE(check_other->first == value4.first);
    ASSERT_TRUE(check_this->first == value3.first);
    check_other++;

    // Both should point to "fee", but they should be different objects
    ASSERT_TRUE(check_this->first == value3.first);
    ASSERT_TRUE(check_other->first == value3.first);
    ASSERT_TRUE(&*check_this != &*check_other);
    check_this++;
    check_other++;

    // Only 'otherStore' should have the 'feed' object
    ASSERT_TRUE(check_other->first == value5.first);
    check_other++;

    // Now both should point to the same "fod" node.
    ASSERT_EQUALS(&*check_this, &*check_other);
    check_this++;
    check_other++;

    // Now both should point to the same "foo" node.
    ASSERT_EQUALS(&*check_this, &*check_other);
    check_this++;
    check_other++;

    ASSERT_TRUE(check_this == thisStore.end());
    ASSERT_TRUE(check_other == otherStore.end());
}

TEST_F(RadixStoreTest, InsertTest6) {
    value_type value1 = std::make_pair("foo", "1");
    value_type value2 = std::make_pair("fod", "2");
    value_type value3 = std::make_pair("feed", "3");
    value_type value4 = std::make_pair("fed", "4");
    value_type value5 = std::make_pair("fee", "5");

    thisStore.insert(value_type(value1));
    thisStore.insert(value_type(value2));
    thisStore.insert(value_type(value3));

    otherStore = thisStore;

    otherStore.insert(value_type(value4));
    otherStore.insert(value_type(value5));

    StringStore::const_iterator check_this = thisStore.begin();
    StringStore::const_iterator check_other = otherStore.begin();

    // Only 'otherStore' should have the 'fed' object, whereas thisStore should point to the 'feed'
    // node
    ASSERT_TRUE(check_other->first == value4.first);
    ASSERT_TRUE(check_this->first == value3.first);
    check_other++;

    // Only 'otherStore' should have the 'fee' object
    ASSERT_TRUE(check_other->first == value5.first);
    check_other++;

    // Now both should point to the same "feed" node.
    ASSERT_EQUALS(&*check_this, &*check_other);
    check_this++;
    check_other++;

    // Now both should point to the same "fod" node.
    ASSERT_EQUALS(&*check_this, &*check_other);
    check_this++;
    check_other++;

    // Now both should point to the same "foo" node.
    ASSERT_EQUALS(&*check_this, &*check_other);
    check_this++;
    check_other++;

    ASSERT_TRUE(check_this == thisStore.end());
    ASSERT_TRUE(check_other == otherStore.end());
}

TEST_F(RadixStoreTest, FindTest) {
    value_type value1 = std::make_pair("foo", "1");
    value_type value2 = std::make_pair("bar", "2");
    value_type value3 = std::make_pair("foozeball", "3");

    thisStore.insert(value_type(value1));
    thisStore.insert(value_type(value2));
    thisStore.insert(value_type(value3));
    auto expected = StringStore::size_type(3);
    ASSERT_EQ(thisStore.size(), expected);

    StringStore::const_iterator iter1 = thisStore.find(value1.first);
    ASSERT_FALSE(iter1 == thisStore.end());
    ASSERT_TRUE(*iter1 == value1);

    StringStore::const_iterator iter2 = thisStore.find("fooze");
    ASSERT_TRUE(iter2 == thisStore.end());
}

TEST_F(RadixStoreTest, UpdateTest) {
    value_type value1 = std::make_pair("foo", "1");
    value_type value2 = std::make_pair("bar", "2");
    value_type value3 = std::make_pair("foz", "3");
    value_type upd = std::make_pair("foo", "test");

    thisStore.insert(value_type(value1));
    thisStore.insert(value_type(value2));
    thisStore.insert(value_type(value3));

    StringStore copy(thisStore);
    thisStore.update(value_type(upd));

    StringStore::const_iterator it2 = thisStore.begin();
    StringStore::const_iterator copy_it2 = copy.begin();

    // both should point to the same 'bar' object
    ASSERT_EQ(&*it2, &*copy_it2);
    it2++;
    copy_it2++;

    // the 'foo' object should be different
    ASSERT_TRUE(it2->second == "test");
    ASSERT_TRUE(copy_it2->second != "test");
    ASSERT_TRUE(&*copy_it2 != &*it2);
    it2++;
    copy_it2++;

    ASSERT_EQ(&*it2, &*copy_it2);
    it2++;
    copy_it2++;

    ASSERT_TRUE(copy_it2 == copy.end());
    ASSERT_TRUE(it2 == thisStore.end());
}

TEST_F(RadixStoreTest, UpdateTest2) {
    value_type value1 = std::make_pair("foo", "1");
    value_type value2 = std::make_pair("bar", "2");
    value_type value3 = std::make_pair("fool", "3");
    value_type upd = std::make_pair("fool", "test");

    thisStore.insert(value_type(value1));
    thisStore.insert(value_type(value2));
    thisStore.insert(value_type(value3));

    StringStore copy(thisStore);
    thisStore.update(value_type(upd));

    StringStore::const_iterator it2 = thisStore.begin();
    StringStore::const_iterator copy_it2 = copy.begin();

    // both should point to the same 'bar' object
    ASSERT_EQ(&*it2, &*copy_it2);
    it2++;
    copy_it2++;

    // the 'foo' object should be different but have the same value. This is due to the node being
    // copied since 'fool' was updated
    ASSERT_TRUE(it2->second == "1");
    ASSERT_TRUE(copy_it2->second == "1");
    ASSERT_TRUE(&*copy_it2 != &*it2);
    it2++;
    copy_it2++;

    // the 'fool' object should be different
    ASSERT_TRUE(it2->second == "test");
    ASSERT_TRUE(copy_it2->second != "test");
    ASSERT_TRUE(&*copy_it2 != &*it2);
    it2++;
    copy_it2++;

    ASSERT_TRUE(copy_it2 == copy.end());
    ASSERT_TRUE(it2 == thisStore.end());
}

TEST_F(RadixStoreTest, UpdateTest3) {
    value_type value1 = std::make_pair("foo", "1");
    value_type value2 = std::make_pair("fod", "2");
    value_type value3 = std::make_pair("fee", "3");
    value_type value4 = std::make_pair("fed", "4");
    value_type value5 = std::make_pair("feed", "5");

    value_type upd_val = std::make_pair("fee", "6");

    thisStore.insert(value_type(value1));
    thisStore.insert(value_type(value2));
    thisStore.insert(value_type(value3));
    thisStore.insert(value_type(value5));

    otherStore = thisStore;

    otherStore.insert(value_type(value4));
    otherStore.update(value_type(upd_val));

    StringStore::const_iterator check_this = thisStore.begin();
    StringStore::const_iterator check_other = otherStore.begin();

    // Only 'otherStore' should have the 'fed' object, whereas thisStore should point to the 'fee'
    // node
    ASSERT_TRUE(check_other->first == value4.first);
    ASSERT_TRUE(check_this->first == value3.first);
    check_other++;

    // 'thisStore' should point to the old 'fee' object whereas 'otherStore' should point to the
    // updated object
    ASSERT_TRUE(check_this->first == value3.first);
    ASSERT_TRUE(check_this->second == value3.second);
    ASSERT_TRUE(check_other->first == value3.first);
    ASSERT_TRUE(check_other->second == upd_val.second);
    ASSERT_TRUE(&*check_this != &*check_other);
    check_this++;
    check_other++;

    // Now both should point to the same "feed" node.
    ASSERT_EQUALS(&*check_this, &*check_other);
    check_this++;
    check_other++;

    // Now both should point to the same "fod" node.
    ASSERT_EQUALS(&*check_this, &*check_other);
    check_this++;
    check_other++;

    // Now both should point to the same "foo" node.
    ASSERT_EQUALS(&*check_this, &*check_other);
    check_this++;
    check_other++;

    ASSERT_TRUE(check_this == thisStore.end());
    ASSERT_TRUE(check_other == otherStore.end());
}

TEST_F(RadixStoreTest, EraseTest) {
    value_type value1 = std::make_pair("abc", "1");
    value_type value2 = std::make_pair("def", "4");
    value_type value3 = std::make_pair("ghi", "5");
    thisStore.insert(value_type(value1));
    thisStore.insert(value_type(value2));
    thisStore.insert(value_type(value3));

    ASSERT_EQ(thisStore.size(), StringStore::size_type(3));

    StringStore::size_type success = thisStore.erase(value1.first);
    ASSERT_TRUE(success);
    ASSERT_EQ(thisStore.size(), StringStore::size_type(2));

    StringStore::const_iterator it = thisStore.begin();
    while (it != thisStore.end()) {
        it++;
    }

    auto iter = thisStore.begin();
    ASSERT_TRUE(*iter == value2);
    ++iter;
    ASSERT_TRUE(*iter == value3);
    ++iter;
    ASSERT_TRUE(iter == thisStore.end());

    ASSERT_FALSE(thisStore.erase("jkl"));
}

TEST_F(RadixStoreTest, ErasePrefixOfAnotherKeyOfCopiedStoreTest) {
    std::string prefix = "bar";
    std::string otherKey = "barrista";
    value_type value1 = std::make_pair(prefix, "2");
    value_type value2 = std::make_pair(otherKey, "3");
    value_type value3 = std::make_pair("foz", "4");
    baseStore.insert(value_type(value1));
    baseStore.insert(value_type(value2));
    baseStore.insert(value_type(value3));

    thisStore = baseStore;
    StringStore::size_type success = thisStore.erase(prefix);

    ASSERT_TRUE(success);
    ASSERT_EQ(thisStore.size(), StringStore::size_type(2));
    ASSERT_EQ(baseStore.size(), StringStore::size_type(3));
    StringStore::const_iterator iter = thisStore.find(otherKey);
    ASSERT_TRUE(iter != thisStore.end());
    ASSERT_EQ(iter->first, otherKey);

    // StringStore::const_iterator baseIter = baseStore.begin();
}

TEST_F(RadixStoreTest, ErasePrefixOfAnotherKeyTest) {
    std::string prefix = "bar";
    std::string otherKey = "barrista";
    value_type value1 = std::make_pair(prefix, "2");
    value_type value2 = std::make_pair(otherKey, "3");
    value_type value3 = std::make_pair("foz", "4");
    thisStore.insert(value_type(value1));
    thisStore.insert(value_type(value2));
    thisStore.insert(value_type(value3));

    ASSERT_EQ(thisStore.size(), StringStore::size_type(3));

    StringStore::size_type success = thisStore.erase(prefix);
    ASSERT_TRUE(success);
    ASSERT_EQ(thisStore.size(), StringStore::size_type(2));
    StringStore::const_iterator iter = thisStore.find(otherKey);
    ASSERT_TRUE(iter != thisStore.end());
    ASSERT_EQ(iter->first, otherKey);
}

TEST_F(RadixStoreTest, EraseKeyWithPrefixStillInStoreTest) {
    std::string key = "barrista";
    std::string prefix = "bar";
    value_type value1 = std::make_pair(prefix, "2");
    value_type value2 = std::make_pair(key, "3");
    value_type value3 = std::make_pair("foz", "4");
    thisStore.insert(value_type(value1));
    thisStore.insert(value_type(value2));
    thisStore.insert(value_type(value3));

    ASSERT_EQ(thisStore.size(), StringStore::size_type(3));

    StringStore::size_type success = thisStore.erase(key);
    ASSERT_TRUE(success);
    ASSERT_EQ(thisStore.size(), StringStore::size_type(2));
    StringStore::const_iterator iter = thisStore.find(prefix);
    ASSERT_FALSE(iter == thisStore.end());
    ASSERT_EQ(iter->first, prefix);
}

TEST_F(RadixStoreTest, EraseKeyThatOverlapsAnotherKeyTest) {
    std::string key = "foo";
    std::string otherKey = "foz";
    value_type value1 = std::make_pair(key, "1");
    value_type value2 = std::make_pair(otherKey, "4");
    value_type value3 = std::make_pair("bar", "5");
    thisStore.insert(value_type(value1));
    thisStore.insert(value_type(value2));
    thisStore.insert(value_type(value3));

    ASSERT_EQ(thisStore.size(), StringStore::size_type(3));

    StringStore::size_type success = thisStore.erase(key);
    ASSERT_TRUE(success);
    ASSERT_EQ(thisStore.size(), StringStore::size_type(2));
    StringStore::const_iterator iter = thisStore.find(otherKey);
    ASSERT_FALSE(iter == thisStore.end());
    ASSERT_EQ(iter->first, otherKey);
}

TEST_F(RadixStoreTest, CopyTest) {
    value_type value1 = std::make_pair("foo", "1");
    value_type value2 = std::make_pair("bar", "2");
    value_type value3 = std::make_pair("foz", "3");
    value_type value4 = std::make_pair("baz", "4");
    thisStore.insert(value_type(value1));
    thisStore.insert(value_type(value2));
    thisStore.insert(value_type(value3));

    StringStore copy(thisStore);

    std::pair<StringStore::const_iterator, bool> ins = copy.insert(value_type(value4));
    StringStore::const_iterator find1 = copy.find(value4.first);
    ASSERT_EQ(&*find1, &*ins.first);

    StringStore::const_iterator find2 = thisStore.find(value4.first);
    ASSERT_TRUE(find2 == thisStore.end());

    StringStore::const_iterator iter = thisStore.begin();
    StringStore::const_iterator copy_iter = copy.begin();

    ASSERT_EQ(&*iter, &*copy_iter);

    iter++;
    copy_iter++;

    ASSERT_TRUE(copy_iter->first == "baz");

    // 'baz' should not be in iter
    ASSERT_FALSE(iter->first == "baz");
    copy_iter++;

    ASSERT_EQ(&*iter, &*copy_iter);

    iter++;
    copy_iter++;
    ASSERT_EQ(&*iter, &*copy_iter);
}

TEST_F(RadixStoreTest, EmptyTest) {
    value_type value1 = std::make_pair("1", "foo");
    ASSERT_TRUE(thisStore.empty());

    thisStore.insert(value_type(value1));
    ASSERT_FALSE(thisStore.empty());
}

TEST_F(RadixStoreTest, NumElementsTest) {
    value_type value1 = std::make_pair("1", "foo");
    auto expected1 = StringStore::size_type(0);
    ASSERT_EQ(thisStore.size(), expected1);

    thisStore.insert(value_type(value1));
    auto expected2 = StringStore::size_type(1);
    ASSERT_EQ(thisStore.size(), expected2);
}

TEST_F(RadixStoreTest, ClearTest) {
    value_type value1 = std::make_pair("1", "foo");

    thisStore.insert(value_type(value1));
    ASSERT_FALSE(thisStore.empty());

    thisStore.clear();
    ASSERT_TRUE(thisStore.empty());
}

TEST_F(RadixStoreTest, DataSizeTest) {
    std::string str1 = "foo";
    std::string str2 = "bar65";

    value_type value1 = std::make_pair("1", str1);
    value_type value2 = std::make_pair("2", str2);
    thisStore.insert(value_type(value1));
    thisStore.insert(value_type(value2));
    ASSERT_EQ(thisStore.dataSize(), str1.size() + str2.size());
}

TEST_F(RadixStoreTest, DistanceTest) {
    value_type value1 = std::make_pair("foo", "1");
    value_type value2 = std::make_pair("bar", "2");
    value_type value3 = std::make_pair("faz", "3");
    value_type value4 = std::make_pair("baz", "4");

    thisStore.insert(value_type(value1));
    thisStore.insert(value_type(value2));
    thisStore.insert(value_type(value3));
    thisStore.insert(value_type(value4));

    StringStore::const_iterator begin = thisStore.begin();
    StringStore::const_iterator second = thisStore.begin();
    ++second;
    StringStore::const_iterator end = thisStore.end();

    ASSERT_EQ(thisStore.distance(begin, end), 4);
    ASSERT_EQ(thisStore.distance(second, end), 3);
}

TEST_F(RadixStoreTest, MergeNoModifications) {
    value_type value1 = std::make_pair("1", "foo");
    value_type value2 = std::make_pair("2", "bar");

    baseStore.insert(value_type(value1));
    baseStore.insert(value_type(value2));

    thisStore = baseStore;
    otherStore = baseStore;

    expected.insert(value_type(value1));
    expected.insert(value_type(value2));

    StringStore merged = thisStore.merge3(baseStore, otherStore);

    ASSERT_TRUE(merged == expected);
}

TEST_F(RadixStoreTest, MergeModifications) {
    value_type value1 = std::make_pair("1", "foo");
    value_type value2 = std::make_pair("1", "bar");

    value_type value3 = std::make_pair("3", "baz");
    value_type value4 = std::make_pair("3", "faz");

    baseStore.insert(value_type(value1));
    baseStore.insert(value_type(value3));

    thisStore = baseStore;
    otherStore = baseStore;

    thisStore.update(value_type(value2));

    otherStore.update(value_type(value4));

    expected.insert(value_type(value2));
    expected.insert(value_type(value4));

    StringStore merged = thisStore.merge3(baseStore, otherStore);

    ASSERT_TRUE(merged == expected);
}

TEST_F(RadixStoreTest, MergeDeletions) {
    value_type value1 = std::make_pair("1", "foo");
    value_type value2 = std::make_pair("2", "moo");
    value_type value3 = std::make_pair("3", "bar");
    value_type value4 = std::make_pair("4", "baz");
    baseStore.insert(value_type(value1));
    baseStore.insert(value_type(value2));
    baseStore.insert(value_type(value3));
    baseStore.insert(value_type(value4));

    thisStore = baseStore;
    otherStore = baseStore;

    thisStore.erase(value2.first);
    otherStore.erase(value4.first);

    expected.insert(value_type(value1));
    expected.insert(value_type(value3));

    StringStore merged = thisStore.merge3(baseStore, otherStore);

    ASSERT_TRUE(merged == expected);
}

TEST_F(RadixStoreTest, MergeInsertions) {
    value_type value1 = std::make_pair("1", "foo");
    value_type value2 = std::make_pair("2", "foo");
    value_type value3 = std::make_pair("3", "bar");
    value_type value4 = std::make_pair("4", "faz");

    baseStore.insert(value_type(value1));
    baseStore.insert(value_type(value2));

    thisStore = baseStore;
    otherStore = baseStore;

    thisStore.insert(value_type(value4));
    otherStore.insert(value_type(value3));

    expected.insert(value_type(value1));
    expected.insert(value_type(value2));
    expected.insert(value_type(value3));
    expected.insert(value_type(value4));

    StringStore merged = thisStore.merge3(baseStore, otherStore);

    ASSERT_TRUE(merged == expected);
}

TEST_F(RadixStoreTest, MergeEmptyInsertionOther) {
    value_type value1 = std::make_pair("1", "foo");

    thisStore = baseStore;
    otherStore = baseStore;

    otherStore.insert(value_type(value1));

    StringStore merged = thisStore.merge3(baseStore, otherStore);

    ASSERT_TRUE(merged == otherStore);
}

TEST_F(RadixStoreTest, MergeEmptyInsertionThis) {
    value_type value1 = std::make_pair("1", "foo");

    thisStore = baseStore;
    otherStore = baseStore;

    thisStore.insert(value_type(value1));

    StringStore merged = thisStore.merge3(baseStore, otherStore);

    ASSERT_TRUE(merged == thisStore);
}

// TEST_F(RadixStoreTest, MergeInsertionDeletionModification) {
// value_type value1 = std::make_pair("1", "foo");
// value_type value2 = std::make_pair("2", "baz");
// value_type value3 = std::make_pair("3", "bar");
// value_type value4 = std::make_pair("4", "faz");
// value_type value5 = std::make_pair("5", "too");
// value_type value6 = std::make_pair("6", "moo");
// value_type value7 = std::make_pair("1", "modified");
// value_type value8 = std::make_pair("2", "modified2");

// baseStore.insert(value_type(value1));
// baseStore.insert(value_type(value2));
// baseStore.insert(value_type(value3));
// baseStore.insert(value_type(value4));

// thisStore = baseStore;
// otherStore = baseStore;

// thisStore.update(value_type(value7));
// thisStore.erase(value4.first);
// thisStore.insert(value_type(value5));

// otherStore.update(value_type(value8));
// otherStore.erase(value3.first);
// std::cout << "done erasing" << std::endl;
// otherStore.insert(value_type(value6));

// expected.insert(value_type(value7));
// expected.insert(value_type(value8));
// expected.insert(value_type(value5));
// expected.insert(value_type(value6));

// std::cout << "pre merge" << std::endl;
// StringStore merged = thisStore.merge3(baseStore, otherStore);
// std::cout << "post merge" << std::endl;

// ASSERT_TRUE(merged == expected);
//}

TEST_F(RadixStoreTest, MergeConflictingModifications) {
    value_type value1 = std::make_pair("1", "foo");
    value_type value2 = std::make_pair("1", "bar");
    value_type value3 = std::make_pair("1", "baz");

    baseStore.insert(value_type(value1));

    thisStore = baseStore;
    otherStore = baseStore;

    thisStore.update(value_type(value2));

    otherStore.update(value_type(value3));

    ASSERT_THROWS(thisStore.merge3(baseStore, otherStore), merge_conflict_exception);
}

TEST_F(RadixStoreTest, MergeConflictingModifictionOtherAndDeletionThis) {
    value_type value1 = std::make_pair("1", "foo");
    value_type value2 = std::make_pair("1", "bar");

    baseStore.insert(value_type(value1));

    thisStore = baseStore;
    otherStore = baseStore;
    thisStore.erase(value1.first);
    otherStore.update(value_type(value2));
    ASSERT_THROWS(thisStore.merge3(baseStore, otherStore), merge_conflict_exception);
}

TEST_F(RadixStoreTest, MergeConflictingModifictionThisAndDeletionOther) {
    value_type value1 = std::make_pair("1", "foo");
    value_type value2 = std::make_pair("1", "bar");

    baseStore.insert(value_type(value1));

    thisStore = baseStore;
    otherStore = baseStore;

    thisStore.update(value_type(value2));

    otherStore.erase(value1.first);

    ASSERT_THROWS(thisStore.merge3(baseStore, otherStore), merge_conflict_exception);
}

TEST_F(RadixStoreTest, MergeConflictingInsertions) {
    value_type value1 = std::make_pair("1", "foo");
    value_type value2 = std::make_pair("1", "foo");

    thisStore = baseStore;
    otherStore = baseStore;

    thisStore.insert(value_type(value2));

    otherStore.insert(value_type(value1));

    ASSERT_THROWS(thisStore.merge3(baseStore, otherStore), merge_conflict_exception);
}

TEST_F(RadixStoreTest, ReverseUpperBoundTest) {
    value_type value1 = std::make_pair("foo", "1");
    value_type value2 = std::make_pair("bar", "2");
    value_type value3 = std::make_pair("baz", "3");
    value_type value4 = std::make_pair("fools", "4");

    thisStore.insert(value_type(value1));
    thisStore.insert(value_type(value2));
    thisStore.insert(value_type(value3));
    thisStore.insert(value_type(value4));

    StringStore::const_iterator iter1 = thisStore.rupper_bound(value4.first);
    ASSERT_EQ(iter1->first, "foo");

    iter1++;
    ASSERT_EQ(iter1->first, "baz");

    StringStore::const_iterator iter2 = thisStore.rupper_bound(value2.first);
    ASSERT_TRUE(iter2 == thisStore.rend());

    StringStore::const_iterator iter3 = thisStore.rupper_bound("dummy_key");
    ASSERT_TRUE(iter3 == thisStore.rend());
}

TEST_F(RadixStoreTest, ReverseLowerBoundTest) {
    value_type value1 = std::make_pair("foo", "1");
    value_type value2 = std::make_pair("bar", "2");
    value_type value3 = std::make_pair("baz", "3");
    value_type value4 = std::make_pair("fools", "4");

    thisStore.insert(value_type(value1));
    thisStore.insert(value_type(value2));
    thisStore.insert(value_type(value3));
    thisStore.insert(value_type(value4));

    StringStore::const_iterator iter1 = thisStore.rlower_bound(value2.first);
    ASSERT_EQ(iter1->first, "bar");

    iter1++;
    ASSERT_TRUE(iter1 == thisStore.rend());

    StringStore::const_iterator iter2 = thisStore.rlower_bound("dummy_key");
    ASSERT_TRUE(iter2 == thisStore.rend());
}

TEST_F(RadixStoreTest, UpperBoundTest) {
    value_type value1 = std::make_pair("foo", "1");
    value_type value2 = std::make_pair("bar", "2");
    value_type value3 = std::make_pair("baz", "3");
    value_type value4 = std::make_pair("fools", "4");

    thisStore.insert(value_type(value1));
    thisStore.insert(value_type(value2));
    thisStore.insert(value_type(value3));
    thisStore.insert(value_type(value4));

    StringStore::const_iterator iter1 = thisStore.upper_bound(value2.first);
    ASSERT_EQ(iter1->first, "baz");
    StringStore::const_iterator iter2 = thisStore.upper_bound(value4.first);
    ASSERT_TRUE(iter2 == thisStore.end());
}

TEST_F(RadixStoreTest, LowerBoundTest) {
    value_type value1 = std::make_pair("foo", "1");
    value_type value2 = std::make_pair("bar", "2");
    value_type value3 = std::make_pair("baz", "3");
    value_type value4 = std::make_pair("fools", "4");

    thisStore.insert(value_type(value1));
    thisStore.insert(value_type(value2));
    thisStore.insert(value_type(value3));
    thisStore.insert(value_type(value4));

    StringStore::const_iterator iter1 = thisStore.lower_bound(value2.first);
    ASSERT_EQ(iter1->first, "bar");

    ++iter1;
    ASSERT_EQ(iter1->first, "baz");

    StringStore::const_iterator iter2 = thisStore.lower_bound("dummy_key");
    ASSERT_TRUE(iter2 == thisStore.end());
}

TEST_F(RadixStoreTest, ReverseIteratorTest) {
    value_type value1 = std::make_pair("foo", "3");
    value_type value2 = std::make_pair("bar", "1");
    value_type value3 = std::make_pair("baz", "2");
    value_type value4 = std::make_pair("fools", "5");
    value_type value5 = std::make_pair("foods", "4");


    thisStore.insert(value_type(value4));
    thisStore.insert(value_type(value5));
    thisStore.insert(value_type(value1));
    thisStore.insert(value_type(value3));
    thisStore.insert(value_type(value2));

    int cur = 5;
    for (auto iter = thisStore.rbegin(); iter != thisStore.rend(); ++iter) {
        ASSERT_EQ(iter->second, std::to_string(cur));
        --cur;
    }
    ASSERT_EQ(cur, 0);
}

}  // namespace
}  // mongo namespace
}  // biggie namespace
