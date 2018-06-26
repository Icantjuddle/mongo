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
namespace biggie {

class StoreTest : public unittest::Test {};

TEST_F(StoreTest, InsertTest) {
    Store<std::string, std::string>::value_type value1 = std::make_pair("1", "foo");
    Store<std::string, std::string> store;
    std::pair<Store<std::string, std::string>::iterator, bool> res =
        store.insert(Store<std::string, std::string>::value_type(value1));
    ASSERT_TRUE(res.second);
    ASSERT_TRUE(*res.first == value1);
}

TEST_F(StoreTest, EmptyTest) {
    Store<std::string, std::string>::value_type value1 = std::make_pair("1", "foo");
    Store<std::string, std::string> store;
    ASSERT_TRUE(store.empty());

    store.insert(Store<std::string, std::string>::value_type(value1));
    ASSERT_FALSE(store.empty());
}

TEST_F(StoreTest, SizeTest) {
    Store<std::string, std::string>::value_type value1 = std::make_pair("1", "foo");
    Store<std::string, std::string> store;
    auto expected1 = Store<std::string, std::string>::size_type(0);
    ASSERT_EQ(store.size(), expected1);

    store.insert(Store<std::string, std::string>::value_type(value1));
    auto expected2 = Store<std::string, std::string>::size_type(1);
    ASSERT_EQ(store.size(), expected2);
}

TEST_F(StoreTest, ClearTest) {
    Store<std::string, std::string>::value_type value1 = std::make_pair("1", "foo");
    Store<std::string, std::string> store;
    store.insert(Store<std::string, std::string>::value_type(value1));
    ASSERT_FALSE(store.empty());

    store.clear();
    ASSERT_TRUE(store.empty());
}

TEST_F(StoreTest, EraseTest) {
    Store<std::string, std::string>::value_type value1 = std::make_pair("1", "foo");
    Store<std::string, std::string>::value_type value2 = std::make_pair("2", "bar");
    Store<std::string, std::string> store;
    store.insert(Store<std::string, std::string>::value_type(value1));
    store.insert(Store<std::string, std::string>::value_type(value2));
    auto expected1 = Store<std::string, std::string>::size_type(2);
    ASSERT_EQ(store.size(), expected1);

    store.erase(value1.first);
    auto expected2 = Store<std::string, std::string>::size_type(1);
    ASSERT_EQ(store.size(), expected2);

    store.erase("3");
    ASSERT_EQ(store.size(), expected2);
}

TEST_F(StoreTest, FindTest) {
    Store<std::string, std::string>::value_type value1 = std::make_pair("1", "foo");
    Store<std::string, std::string>::value_type value2 = std::make_pair("2", "bar");
    Store<std::string, std::string> store;
    store.insert(Store<std::string, std::string>::value_type(value1));
    store.insert(Store<std::string, std::string>::value_type(value2));
    auto expected = Store<std::string, std::string>::size_type(2);
    ASSERT_EQ(store.size(), expected);

    Store<std::string, std::string>::iterator iter1 = store.find(value1.first);
    ASSERT_TRUE(*iter1 == value1);

    Store<std::string, std::string>::iterator iter2 = store.find("3");
    ASSERT_TRUE(iter2 == store.end());
}

TEST_F(StoreTest, DataSizeTest) {
    std::string str1 = "foo";
    std::string str2 = "bar65";

    Store<std::string, std::string>::value_type value1 = std::make_pair("1", str1);
    Store<std::string, std::string>::value_type value2 = std::make_pair("2", str2);
    Store<std::string, std::string> store;
    store.insert(Store<std::string, std::string>::value_type(value1));
    store.insert(Store<std::string, std::string>::value_type(value2));
    ASSERT_EQ(store.dataSize(), str1.size() + str2.size());
}

TEST_F(StoreTest, DistanceTest) {
    Store<std::string, std::string>::value_type value1 = std::make_pair("1", "foo");
    Store<std::string, std::string>::value_type value2 = std::make_pair("2", "bar");
    Store<std::string, std::string>::value_type value3 = std::make_pair("3", "foo");
    Store<std::string, std::string>::value_type value4 = std::make_pair("4", "bar");

    Store<std::string, std::string> base;
    base.insert(Store<std::string, std::string>::value_type(value1));
    base.insert(Store<std::string, std::string>::value_type(value2));
    base.insert(Store<std::string, std::string>::value_type(value3));
    base.insert(Store<std::string, std::string>::value_type(value4));

    Store<std::string, std::string>::iterator begin = base.begin();
    Store<std::string, std::string>::iterator second = base.begin();
    ++second;
    Store<std::string, std::string>::iterator end = base.end();

    ASSERT_EQ(base.distance(begin, end), 4);
    ASSERT_EQ(base.distance(second, end), 3);
}

TEST_F(StoreTest, MergeNoModifications) {
    Store<std::string, std::string>::value_type value1 = std::make_pair("1", "foo");
    Store<std::string, std::string>::value_type value2 = std::make_pair("2", "bar");

    Store<std::string, std::string> store1;
    store1.insert(Store<std::string, std::string>::value_type(value1));
    store1.insert(Store<std::string, std::string>::value_type(value2));

    Store<std::string, std::string> store2;
    store2.insert(Store<std::string, std::string>::value_type(value1));
    store2.insert(Store<std::string, std::string>::value_type(value2));

    Store<std::string, std::string> base;
    base.insert(Store<std::string, std::string>::value_type(value1));
    base.insert(Store<std::string, std::string>::value_type(value2));

    Store<std::string, std::string> merged = store1.merge3(base, store2);

    ASSERT_TRUE(merged == store1);
}

TEST_F(StoreTest, MergeModifications) {
    Store<std::string, std::string>::value_type value1 = std::make_pair("1", "foo");
    Store<std::string, std::string>::value_type value2 = std::make_pair("1", "bar");

    Store<std::string, std::string>::value_type value3 = std::make_pair("3", "baz");
    Store<std::string, std::string>::value_type value4 = std::make_pair("3", "faz");

    Store<std::string, std::string> store1;
    store1.insert(Store<std::string, std::string>::value_type(value2));
    store1.insert(Store<std::string, std::string>::value_type(value3));

    Store<std::string, std::string> store2;
    store2.insert(Store<std::string, std::string>::value_type(value1));
    store2.insert(Store<std::string, std::string>::value_type(value4));

    Store<std::string, std::string> base;
    base.insert(Store<std::string, std::string>::value_type(value1));
    base.insert(Store<std::string, std::string>::value_type(value3));

    Store<std::string, std::string> expected;
    expected.insert(Store<std::string, std::string>::value_type(value2));
    expected.insert(Store<std::string, std::string>::value_type(value4));

    Store<std::string, std::string> merged = store1.merge3(base, store2);

    ASSERT_TRUE(merged == expected);
}

TEST_F(StoreTest, MergeDeletions) {
    Store<std::string, std::string>::value_type value1 = std::make_pair("1", "foo");
    Store<std::string, std::string>::value_type value2 = std::make_pair("2", "moo");
    Store<std::string, std::string>::value_type value3 = std::make_pair("3", "bar");
    Store<std::string, std::string>::value_type value4 = std::make_pair("4", "baz");

    Store<std::string, std::string> store1;
    store1.insert(Store<std::string, std::string>::value_type(value1));
    store1.insert(Store<std::string, std::string>::value_type(value3));
    store1.insert(Store<std::string, std::string>::value_type(value4));

    Store<std::string, std::string> store2;
    store2.insert(Store<std::string, std::string>::value_type(value1));
    store2.insert(Store<std::string, std::string>::value_type(value2));
    store2.insert(Store<std::string, std::string>::value_type(value3));

    Store<std::string, std::string> base;
    base.insert(Store<std::string, std::string>::value_type(value1));
    base.insert(Store<std::string, std::string>::value_type(value2));
    base.insert(Store<std::string, std::string>::value_type(value3));
    base.insert(Store<std::string, std::string>::value_type(value4));

    Store<std::string, std::string> expected;
    expected.insert(Store<std::string, std::string>::value_type(value1));
    expected.insert(Store<std::string, std::string>::value_type(value3));

    Store<std::string, std::string> merged = store1.merge3(base, store2);

    ASSERT_TRUE(merged == expected);
}

TEST_F(StoreTest, MergeInsertions) {
    Store<std::string, std::string>::value_type value1 = std::make_pair("1", "foo");
    Store<std::string, std::string>::value_type value2 = std::make_pair("2", "foo");
    Store<std::string, std::string>::value_type value3 = std::make_pair("3", "bar");
    Store<std::string, std::string>::value_type value4 = std::make_pair("4", "faz");

    Store<std::string, std::string> store1;
    store1.insert(Store<std::string, std::string>::value_type(value1));
    store1.insert(Store<std::string, std::string>::value_type(value2));
    store1.insert(Store<std::string, std::string>::value_type(value4));

    Store<std::string, std::string> store2;
    store2.insert(Store<std::string, std::string>::value_type(value1));
    store2.insert(Store<std::string, std::string>::value_type(value2));
    store2.insert(Store<std::string, std::string>::value_type(value3));

    Store<std::string, std::string> base;
    base.insert(Store<std::string, std::string>::value_type(value1));
    base.insert(Store<std::string, std::string>::value_type(value2));

    Store<std::string, std::string> expected;
    expected.insert(Store<std::string, std::string>::value_type(value1));
    expected.insert(Store<std::string, std::string>::value_type(value2));
    expected.insert(Store<std::string, std::string>::value_type(value3));
    expected.insert(Store<std::string, std::string>::value_type(value4));

    Store<std::string, std::string> merged = store1.merge3(base, store2);

    ASSERT_TRUE(merged == expected);
}

TEST_F(StoreTest, MergeEmptyInsertionOther) {
    Store<std::string, std::string>::value_type value1 = std::make_pair("1", "foo");

    Store<std::string, std::string> thisStore;

    Store<std::string, std::string> otherStore;
    otherStore.insert(Store<std::string, std::string>::value_type(value1));

    Store<std::string, std::string> baseStore;

    Store<std::string, std::string> merged = thisStore.merge3(baseStore, otherStore);

    ASSERT_TRUE(merged == otherStore);
}

TEST_F(StoreTest, MergeEmptyInsertionThis) {
    Store<std::string, std::string>::value_type value1 = std::make_pair("1", "foo");

    Store<std::string, std::string> thisStore;
    thisStore.insert(Store<std::string, std::string>::value_type(value1));

    Store<std::string, std::string> otherStore;

    Store<std::string, std::string> baseStore;

    Store<std::string, std::string> merged = thisStore.merge3(baseStore, otherStore);

    ASSERT_TRUE(merged == thisStore);
}

TEST_F(StoreTest, MergeInsertionDeletionModification) {
    Store<std::string, std::string>::value_type value1 = std::make_pair("1", "foo");
    Store<std::string, std::string>::value_type value2 = std::make_pair("2", "baz");
    Store<std::string, std::string>::value_type value3 = std::make_pair("3", "bar");
    Store<std::string, std::string>::value_type value4 = std::make_pair("4", "faz");
    Store<std::string, std::string>::value_type value5 = std::make_pair("5", "too");
    Store<std::string, std::string>::value_type value6 = std::make_pair("6", "moo");
    Store<std::string, std::string>::value_type value7 = std::make_pair("1", "modified");
    Store<std::string, std::string>::value_type value8 = std::make_pair("2", "modified2");

    Store<std::string, std::string> store1;
    store1.insert(Store<std::string, std::string>::value_type(value7));
    store1.insert(Store<std::string, std::string>::value_type(value2));
    store1.insert(Store<std::string, std::string>::value_type(value3));
    store1.insert(Store<std::string, std::string>::value_type(value5));

    Store<std::string, std::string> store2;
    store2.insert(Store<std::string, std::string>::value_type(value1));
    store2.insert(Store<std::string, std::string>::value_type(value8));
    store2.insert(Store<std::string, std::string>::value_type(value4));
    store2.insert(Store<std::string, std::string>::value_type(value6));

    Store<std::string, std::string> base;
    base.insert(Store<std::string, std::string>::value_type(value1));
    base.insert(Store<std::string, std::string>::value_type(value2));
    base.insert(Store<std::string, std::string>::value_type(value3));
    base.insert(Store<std::string, std::string>::value_type(value4));

    Store<std::string, std::string> expected;
    expected.insert(Store<std::string, std::string>::value_type(value7));
    expected.insert(Store<std::string, std::string>::value_type(value8));
    expected.insert(Store<std::string, std::string>::value_type(value5));
    expected.insert(Store<std::string, std::string>::value_type(value6));

    Store<std::string, std::string> merged = store1.merge3(base, store2);

    ASSERT_TRUE(merged == expected);
}

TEST_F(StoreTest, MergeConflictingModifications) {
    Store<std::string, std::string>::value_type value1 = std::make_pair("1", "foo");
    Store<std::string, std::string>::value_type value2 = std::make_pair("1", "bar");
    Store<std::string, std::string>::value_type value3 = std::make_pair("1", "baz");

    Store<std::string, std::string> store1;
    store1.insert(Store<std::string, std::string>::value_type(value2));

    Store<std::string, std::string> store2;
    store2.insert(Store<std::string, std::string>::value_type(value3));

    Store<std::string, std::string> base;
    base.insert(Store<std::string, std::string>::value_type(value1));

    ASSERT_THROWS(store1.merge3(base, store2), merge_conflict_exception);
}

TEST_F(StoreTest, MergeConflictingModifictionOtherAndDeletionThis) {
    Store<std::string, std::string>::value_type value1 = std::make_pair("1", "foo");
    Store<std::string, std::string>::value_type value2 = std::make_pair("1", "bar");

    Store<std::string, std::string> thisStore;

    Store<std::string, std::string> otherStore;
    otherStore.insert(Store<std::string, std::string>::value_type(value2));

    Store<std::string, std::string> baseStore;
    baseStore.insert(Store<std::string, std::string>::value_type(value1));

    ASSERT_THROWS(thisStore.merge3(baseStore, otherStore), merge_conflict_exception);
}

TEST_F(StoreTest, MergeConflictingModifictionThisAndDeletionOther) {
    Store<std::string, std::string>::value_type value1 = std::make_pair("1", "foo");
    Store<std::string, std::string>::value_type value2 = std::make_pair("1", "bar");

    Store<std::string, std::string> thisStore;
    thisStore.insert(Store<std::string, std::string>::value_type(value2));

    Store<std::string, std::string> otherStore;

    Store<std::string, std::string> baseStore;
    baseStore.insert(Store<std::string, std::string>::value_type(value1));

    ASSERT_THROWS(thisStore.merge3(baseStore, otherStore), merge_conflict_exception);
}

TEST_F(StoreTest, MergeConflictingInsertions) {
    Store<std::string, std::string>::value_type value1 = std::make_pair("1", "foo");
    Store<std::string, std::string>::value_type value2 = std::make_pair("1", "bar");

    Store<std::string, std::string> store1;
    store1.insert(Store<std::string, std::string>::value_type(value2));

    Store<std::string, std::string> store2;
    store2.insert(Store<std::string, std::string>::value_type(value1));

    Store<std::string, std::string> base;

    ASSERT_THROWS(store1.merge3(base, store2), merge_conflict_exception);
}

TEST_F(StoreTest, UpperBoundTest) {
    Store<std::string, std::string>::value_type value1 = std::make_pair("1", "foo");
    Store<std::string, std::string>::value_type value2 = std::make_pair("2", "bar");
    Store<std::string, std::string>::value_type value3 = std::make_pair("3", "foo");
    Store<std::string, std::string>::value_type value4 = std::make_pair("5", "bar");

    Store<std::string, std::string> base;
    base.insert(Store<std::string, std::string>::value_type(value1));
    base.insert(Store<std::string, std::string>::value_type(value2));
    base.insert(Store<std::string, std::string>::value_type(value3));
    base.insert(Store<std::string, std::string>::value_type(value4));

    Store<std::string, std::string>::iterator iter1 = base.upper_bound(value2.first);
    ASSERT_EQ(iter1->first, "3");
    Store<std::string, std::string>::iterator iter2 = base.upper_bound(value4.first);
    ASSERT_TRUE(iter2 == base.end());
}

TEST_F(StoreTest, LowerBoundTest) {
    Store<std::string, std::string>::value_type value1 = std::make_pair("1", "foo");
    Store<std::string, std::string>::value_type value2 = std::make_pair("2", "bar");
    Store<std::string, std::string>::value_type value3 = std::make_pair("3", "foo");
    Store<std::string, std::string>::value_type value4 = std::make_pair("5", "bar");

    Store<std::string, std::string> base;
    base.insert(Store<std::string, std::string>::value_type(value1));
    base.insert(Store<std::string, std::string>::value_type(value2));
    base.insert(Store<std::string, std::string>::value_type(value3));
    base.insert(Store<std::string, std::string>::value_type(value4));

    Store<std::string, std::string>::iterator iter1 = base.lower_bound(value2.first);
    ASSERT_EQ(iter1->first, "2");
    Store<std::string, std::string>::iterator iter2 = base.lower_bound("7");
    ASSERT_TRUE(iter2 == base.end());
}

TEST_F(StoreTest, ReverseIteratorTest) {
    Store<std::string, std::string>::value_type value1 = std::make_pair("1", "foo");
    Store<std::string, std::string>::value_type value2 = std::make_pair("2", "bar");
    Store<std::string, std::string>::value_type value3 = std::make_pair("3", "foo");
    Store<std::string, std::string>::value_type value4 = std::make_pair("4", "bar");

    Store<std::string, std::string> base;
    base.insert(Store<std::string, std::string>::value_type(value4));
    base.insert(Store<std::string, std::string>::value_type(value1));
    base.insert(Store<std::string, std::string>::value_type(value3));
    base.insert(Store<std::string, std::string>::value_type(value2));

    int cur = 4;
    for (auto iter = base.rbegin(); iter != base.rend(); ++iter) {
        ASSERT_EQ(iter->first, std::to_string(cur));
        --cur;
    }
}
}  // namespace biggie
}  // namespace mongo
