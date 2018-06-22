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

#include "mongo/platform/basic.h"

#include "mongo/db/storage/biggie/store.h"

#include "mongo/unittest/unittest.h"

namespace mongo {

class StoreTest : public unittest::Test {};

TEST_F(StoreTest, EmptyTest) {
    Store::value_type value1 = std::make_pair("1", "foo");
    Store store;
    ASSERT_TRUE(store.empty());

    store.insert(std::pair<const Key, Store::mapped_type>(value1));
    ASSERT_FALSE(store.empty());
}

TEST_F(StoreTest, SizeTest) {
    Store::value_type value1 = std::make_pair("1", "foo");
    Store store;
    ASSERT_EQ(store.size(), Store::size_type(0));

    store.insert(std::pair<const Key, Store::mapped_type>(value1));
    ASSERT_EQ(store.size(), Store::size_type(1));
}

TEST_F(StoreTest, ClearTest) {
    Store::value_type value1 = std::make_pair("1", "foo");
    Store store;
    store.insert(std::pair<const Key, Store::mapped_type>(value1));
    ASSERT_FALSE(store.empty());

    store.clear();
    ASSERT_TRUE(store.empty());
}

TEST_F(StoreTest, InsertTest) {
    Store::value_type value1 = std::make_pair("1", "foo");
    Store store;
    std::pair<Store::Iterator, bool> res =
        store.insert(std::pair<const Key, Store::mapped_type>(value1));
    ASSERT_TRUE(res.second);
    ASSERT_TRUE(*res.first == value1);
}

TEST_F(StoreTest, EraseTest) {
    Store::value_type value1 = std::make_pair("1", "foo");
    Store::value_type value2 = std::make_pair("2", "bar");
    Store store;
    store.insert(std::pair<const Key, Store::mapped_type>(value1));
    store.insert(std::pair<const Key, Store::mapped_type>(value2));
    ASSERT_EQ(store.size(), Store::size_type(2));

    store.erase(value1.first);
    ASSERT_EQ(store.size(), Store::size_type(1));
}

TEST_F(StoreTest, FindTest) {
    Store::value_type value1 = std::make_pair("1", "foo");
    Store::value_type value2 = std::make_pair("2", "bar");
    Store store;
    store.insert(std::pair<const Key, Store::mapped_type>(value1));
    store.insert(std::pair<const Key, Store::mapped_type>(value2));
    ASSERT_EQ(store.size(), Store::size_type(2));

    Store::Iterator iter = store.find(value1.first);
    ASSERT_TRUE(*iter == value1);
}

TEST_F(StoreTest, DataSizeTest) {
    std::string str1 = "foo";
    std::string str2 = "bar65";

    Store::value_type value1 = std::make_pair("1", str1);
    Store::value_type value2 = std::make_pair("2", str2);
    Store store;
    store.insert(std::pair<const Key, Store::mapped_type>(value1));
    store.insert(std::pair<const Key, Store::mapped_type>(value2));
    ASSERT_EQ(store.dataSize(), str1.size() + str2.size());
}

TEST_F(StoreTest, DistanceTest) {
    Store::value_type value1 = std::make_pair("1", "foo");
    Store::value_type value2 = std::make_pair("2", "bar");
    Store::value_type value3 = std::make_pair("3", "foo");
    Store::value_type value4 = std::make_pair("4", "bar");

    Store base;
    base.insert(std::pair<const Key, Store::mapped_type>(value1));
    base.insert(std::pair<const Key, Store::mapped_type>(value2));
    base.insert(std::pair<const Key, Store::mapped_type>(value3));
    base.insert(std::pair<const Key, Store::mapped_type>(value4));

    Store::Iterator begin = base.begin();
    Store::Iterator second = base.begin();
    ++second;
    Store::Iterator end = base.end();

    ASSERT_EQ(base.distance(begin, end), 4);
    ASSERT_EQ(base.distance(second, end), 3);
}

TEST_F(StoreTest, MergeNoModifications) {
    Store::value_type value1 = std::make_pair("1", "foo");
    Store::value_type value2 = std::make_pair("2", "bar");

    Store store1;
    store1.insert(std::pair<const Key, Store::mapped_type>(value1));
    store1.insert(std::pair<const Key, Store::mapped_type>(value2));

    Store store2;
    store2.insert(std::pair<const Key, Store::mapped_type>(value1));
    store2.insert(std::pair<const Key, Store::mapped_type>(value2));

    Store base;
    base.insert(std::pair<const Key, Store::mapped_type>(value1));
    base.insert(std::pair<const Key, Store::mapped_type>(value2));

    Store::Store& merged = store1.merge3(base, store2);

    ASSERT_TRUE(merged == store1);
}

TEST_F(StoreTest, MergeModifications) {
    Store::value_type value1 = std::make_pair("1", "foo");
    Store::value_type value2 = std::make_pair("2", "bar");

    Store::value_type value3 = std::make_pair("3", "baz");
    Store::value_type value4 = std::make_pair("4", "faz");

    Store store1;
    store1.insert(std::pair<const Key, Store::mapped_type>(value2));
    store1.insert(std::pair<const Key, Store::mapped_type>(value3));

    Store store2;
    store2.insert(std::pair<const Key, Store::mapped_type>(value1));
    store2.insert(std::pair<const Key, Store::mapped_type>(value4));

    Store base;
    base.insert(std::pair<const Key, Store::mapped_type>(value1));
    base.insert(std::pair<const Key, Store::mapped_type>(value3));

    Store expected;
    expected.insert(std::pair<const Key, Store::mapped_type>(value2));
    expected.insert(std::pair<const Key, Store::mapped_type>(value4));

    Store::Store& merged = store1.merge3(base, store2);

    ASSERT_TRUE(merged == expected);
}

TEST_F(StoreTest, MergeDeletions) {
    Store::value_type value1 = std::make_pair("1", "foo");
    Store::value_type value2 = std::make_pair("2", "moo");
    Store::value_type value3 = std::make_pair("3", "bar");
    Store::value_type value4 = std::make_pair("4", "baz");

    Store store1;
    store1.insert(std::pair<const Key, Store::mapped_type>(value1));
    store1.insert(std::pair<const Key, Store::mapped_type>(value3));
    store1.insert(std::pair<const Key, Store::mapped_type>(value4));

    Store store2;
    store2.insert(std::pair<const Key, Store::mapped_type>(value1));
    store2.insert(std::pair<const Key, Store::mapped_type>(value2));
    store2.insert(std::pair<const Key, Store::mapped_type>(value3));

    Store base;
    base.insert(std::pair<const Key, Store::mapped_type>(value1));
    base.insert(std::pair<const Key, Store::mapped_type>(value2));
    base.insert(std::pair<const Key, Store::mapped_type>(value3));
    base.insert(std::pair<const Key, Store::mapped_type>(value4));

    Store expected;
    expected.insert(std::pair<const Key, Store::mapped_type>(value1));
    expected.insert(std::pair<const Key, Store::mapped_type>(value3));

    Store::Store& merged = store1.merge3(base, store2);

    ASSERT_TRUE(merged == expected);
}

TEST_F(StoreTest, MergeInsertions) {
    Store::value_type value1 = std::make_pair("1", "foo");
    Store::value_type value2 = std::make_pair("2", "foo");
    Store::value_type value3 = std::make_pair("3", "bar");
    Store::value_type value4 = std::make_pair("4", "faz");

    Store store1;
    store1.insert(std::pair<const Key, Store::mapped_type>(value1));
    store1.insert(std::pair<const Key, Store::mapped_type>(value2));
    store1.insert(std::pair<const Key, Store::mapped_type>(value4));

    Store store2;
    store2.insert(std::pair<const Key, Store::mapped_type>(value1));
    store2.insert(std::pair<const Key, Store::mapped_type>(value2));
    store2.insert(std::pair<const Key, Store::mapped_type>(value3));

    Store base;
    base.insert(std::pair<const Key, Store::mapped_type>(value1));
    base.insert(std::pair<const Key, Store::mapped_type>(value2));

    Store expected;
    expected.insert(std::pair<const Key, Store::mapped_type>(value1));
    expected.insert(std::pair<const Key, Store::mapped_type>(value2));
    expected.insert(std::pair<const Key, Store::mapped_type>(value3));
    expected.insert(std::pair<const Key, Store::mapped_type>(value4));

    Store::Store& merged = store1.merge3(base, store2);

    ASSERT_TRUE(merged == expected);
}

TEST_F(StoreTest, MergeEmptyInsertionOther) {
    Store::value_type value1 = std::make_pair("1", "foo");

    Store store1;

    Store store2;
    store2.insert(std::pair<const Key, Store::mapped_type>(value1));

    Store base;

    Store::Store& merged = store1.merge3(base, store2);

    ASSERT_TRUE(merged == store2);
}

TEST_F(StoreTest, MergeEmptyInsertionThis) {
    Store::value_type value1 = std::make_pair("1", "foo");

    Store store1;
    store1.insert(std::pair<const Key, Store::mapped_type>(value1));

    Store store2;

    Store base;

    Store::Store& merged = store1.merge3(base, store2);

    ASSERT_TRUE(merged == store1);
}

TEST_F(StoreTest, MergeInsertionDeletionModification) {
    Store::value_type value1 = std::make_pair("1", "foo");
    Store::value_type value2 = std::make_pair("2", "baz");
    Store::value_type value3 = std::make_pair("3", "bar");
    Store::value_type value4 = std::make_pair("4", "faz");
    Store::value_type value5 = std::make_pair("5", "too");
    Store::value_type value6 = std::make_pair("6", "moo");
    Store::value_type value7 = std::make_pair("1", "modified");
    Store::value_type value8 = std::make_pair("2", "modified2");

    Store store1;
    store1.insert(std::pair<const Key, Store::mapped_type>(value7));
    store1.insert(std::pair<const Key, Store::mapped_type>(value2));
    store1.insert(std::pair<const Key, Store::mapped_type>(value3));
    store1.insert(std::pair<const Key, Store::mapped_type>(value5));

    Store store2;
    store2.insert(std::pair<const Key, Store::mapped_type>(value1));
    store2.insert(std::pair<const Key, Store::mapped_type>(value8));
    store2.insert(std::pair<const Key, Store::mapped_type>(value4));
    store2.insert(std::pair<const Key, Store::mapped_type>(value6));

    Store base;
    base.insert(std::pair<const Key, Store::mapped_type>(value1));
    base.insert(std::pair<const Key, Store::mapped_type>(value2));
    base.insert(std::pair<const Key, Store::mapped_type>(value3));
    base.insert(std::pair<const Key, Store::mapped_type>(value4));

    Store expected;
    expected.insert(std::pair<const Key, Store::mapped_type>(value7));
    expected.insert(std::pair<const Key, Store::mapped_type>(value8));
    expected.insert(std::pair<const Key, Store::mapped_type>(value5));
    expected.insert(std::pair<const Key, Store::mapped_type>(value6));

    Store::Store& merged = store1.merge3(base, store2);

    ASSERT_TRUE(merged == expected);
}

TEST_F(StoreTest, MergeConflictingModifications) {
    Store::value_type value1 = std::make_pair("1", "foo");
    Store::value_type value2 = std::make_pair("1", "bar");
    Store::value_type value3 = std::make_pair("1", "baz");

    Store store1;
    store1.insert(std::pair<const Key, Store::mapped_type>(value2));

    Store store2;
    store2.insert(std::pair<const Key, Store::mapped_type>(value3));

    Store base;
    base.insert(std::pair<const Key, Store::mapped_type>(value1));

    ASSERT_THROWS(store1.merge3(base, store2), merge_conflict_exception);
}

TEST_F(StoreTest, MergeConflictingModifictionThisAndDeletionOther) {
    Store::value_type value1 = std::make_pair("1", "foo");
    Store::value_type value2 = std::make_pair("1", "bar");

    Store store1;

    Store store2;
    store2.insert(std::pair<const Key, Store::mapped_type>(value2));

    Store base;
    base.insert(std::pair<const Key, Store::mapped_type>(value1));

    ASSERT_THROWS(store1.merge3(base, store2), merge_conflict_exception);
}

TEST_F(StoreTest, MergeConflictingModifictionOtherAndDeletionThis) {
    Store::value_type value1 = std::make_pair("1", "foo");
    Store::value_type value2 = std::make_pair("1", "bar");

    Store store1;
    store1.insert(std::pair<const Key, Store::mapped_type>(value2));

    Store store2;

    Store base;
    base.insert(std::pair<const Key, Store::mapped_type>(value1));

    ASSERT_THROWS(store1.merge3(base, store2), merge_conflict_exception);
}

TEST_F(StoreTest, MergeConflictingInsertions) {
    Store::value_type value1 = std::make_pair("1", "foo");
    Store::value_type value2 = std::make_pair("1", "bar");

    Store store1;
    store1.insert(std::pair<const Key, Store::mapped_type>(value2));

    Store store2;
    store2.insert(std::pair<const Key, Store::mapped_type>(value1));

    Store base;

    ASSERT_THROWS(store1.merge3(base, store2), merge_conflict_exception);
}
}  // namespace
