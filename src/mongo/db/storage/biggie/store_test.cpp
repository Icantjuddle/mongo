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

std::pair<const Key, Store::Mapped> makeValue(uint8_t* a, std::string s) {
    const Key key1 = std::make_pair(a, sizeof(uint8_t));
    return std::make_pair(key1, s);
}

TEST_F(StoreTest, EmptyTest) {
    uint8_t a = 1;
    Store::Value value1 = makeValue(&a, "foo");
    Store store;
    ASSERT_TRUE(store.empty());

    store.insert(std::move(value1));
    ASSERT_FALSE(store.empty());
}

TEST_F(StoreTest, SizeTest) {
    uint8_t a = 1;
    Store::Value value1 = makeValue(&a, "foo");
    Store store;
    ASSERT_EQ(store.size(), Store::Size(0));

    store.insert(std::move(value1));
    ASSERT_EQ(store.size(), Store::Size(1));
}

TEST_F(StoreTest, ClearTest) {
    uint8_t a = 1;
    Store::Value value1 = makeValue(&a, "foo");
    Store store;
    store.insert(std::move(value1));
    ASSERT_FALSE(store.empty());

    store.clear();
    ASSERT_TRUE(store.empty());
}

TEST_F(StoreTest, InsertTest) {
    uint8_t a = 1;
    Store::Value value1 = makeValue(&a, "foo");
    Store store;
    std::pair<Store::Iterator, bool> res = store.insert(std::move(value1));
    ASSERT_TRUE(res.second);
    ASSERT_TRUE(*res.first == value1);
}

TEST_F(StoreTest, EraseTest) {
    uint8_t a = 1;
    uint8_t b = 2;
    Store::Value value1 = makeValue(&a, "foo");
    Store::Value value2 = makeValue(&b, "bar");
    Store store;
    store.insert(std::move(value1));
    store.insert(std::move(value2));
    ASSERT_EQ(store.size(), Store::Size(2));

    store.erase(value1.first);
    ASSERT_EQ(store.size(), Store::Size(1));
}

TEST_F(StoreTest, FindTest) {
    uint8_t a = 1;
    uint8_t b = 2;
    Store::Value value1 = makeValue(&a, "foo");
    Store::Value value2 = makeValue(&b, "bar");
    Store store;
    store.insert(std::move(value1));
    store.insert(std::move(value2));
    ASSERT_EQ(store.size(), Store::Size(2));

    Store::Iterator iter = store.find(value1.first);
    ASSERT_TRUE(*iter == value1);
}

TEST_F(StoreTest, DataSizeTest) {
    uint8_t a = 1;
    uint8_t b = 2;
    std::string str1 = "foo";
    std::string str2 = "bar65";

    Store::Value value1 = makeValue(&a, str1);
    Store::Value value2 = makeValue(&b, str2);
    Store store;
    store.insert(std::move(value1));
    store.insert(std::move(value2));
    ASSERT_EQ(store.dataSize(), str1.size() + str2.size());
}

TEST_F(StoreTest, TotalSizeTest) {
    uint8_t a = 1;
    uint8_t b = 2;
    std::string str1 = "foo";
    std::string str2 = "bar6";

    Store::Value value1 = makeValue(&a, str1);
    Store::Value value2 = makeValue(&b, str2);
    Store store;
    store.insert(std::move(value1));
    store.insert(std::move(value2));

    Store::Size expected = str1.size() + str2.size() + sizeof(uint8_t) * 2 +
        sizeof(std::string) * 2 + sizeof(std::pair<const Key, Store::Mapped>) * 2 +
        sizeof(std::pair<uint8_t*, size_t>) * 2 + sizeof(uint8_t*) * 2;
    ASSERT_EQ(store.totalSize(), expected);
}

TEST_F(StoreTest, GetPrefixTest2) {
    uint8_t pre[] = {1};

    uint8_t first[] = {1, 2};
    uint8_t second[] = {1, 3};
    uint8_t third[] = {3, 4};

    const Key key1 = std::make_pair(first, sizeof(uint8_t) * 2);
    const Key key2 = std::make_pair(second, sizeof(uint8_t) * 2);
    const Key key3 = std::make_pair(third, sizeof(uint8_t) * 2);
    Store::Value value1 = std::make_pair(key1, "foo");
    Store::Value value2 = std::make_pair(key2, "bar");
    Store::Value value3 = std::make_pair(key3, "foo");

    Store base;
    base.insert(std::move(value1));
    base.insert(std::move(value2));
    base.insert(std::move(value3));

    Store expected;
    expected.insert(std::move(value1));
    expected.insert(std::move(value2));

    const Key key = std::make_pair(pre, sizeof(uint8_t));
    Store& get = base.getPrefix(std::move(key));

    ASSERT_EQ(base.size(), Store::Size(3));
    ASSERT_EQ(get.size(), Store::Size(2));
    ASSERT_TRUE(get == expected);
}

TEST_F(StoreTest, RangeScanTest) {
    uint8_t a = 1;
    uint8_t b = 3;
    uint8_t c = 4;
    uint8_t d = 5;
    Store::Value value1 = makeValue(&a, "foo");
    Store::Value value2 = makeValue(&b, "bar");
    Store::Value value3 = makeValue(&c, "foo");
    Store::Value value4 = makeValue(&d, "bar");

    Store base;
    base.insert(std::move(value1));
    base.insert(std::move(value2));
    base.insert(std::move(value3));
    base.insert(std::move(value4));

    uint8_t e = 3;
    const Key key1 = std::make_pair(&e, sizeof(uint8_t));
    const Key key2 = std::make_pair(&d, sizeof(uint8_t));
    ASSERT_EQ(base.rangeScan(key1, key2), Store::Size(2));
}

TEST_F(StoreTest, MergeNoModifications) {
    uint8_t a = 1;
    uint8_t b = 2;
    Store::Value value1 = makeValue(&a, "foo");
    Store::Value value2 = makeValue(&b, "bar");

    Store store1;
    store1.insert(std::move(value1));
    store1.insert(std::move(value2));

    Store store2;
    store2.insert(std::move(value1));
    store2.insert(std::move(value2));

    Store base;
    base.insert(std::move(value1));
    base.insert(std::move(value2));

    Store::Store& merged = store1.merge3(base, store2);

    ASSERT_TRUE(merged == store1);
}

TEST_F(StoreTest, MergeModifications) {
    uint8_t a = 1;
    uint8_t b = 1;
    Store::Value value1 = makeValue(&a, "foo");
    Store::Value value2 = makeValue(&b, "bar");

    uint8_t c = 2;
    uint8_t d = 2;
    Store::Value value3 = makeValue(&c, "baz");
    Store::Value value4 = makeValue(&d, "faz");

    Store store1;
    store1.insert(std::move(value2));
    store1.insert(std::move(value3));

    Store store2;
    store2.insert(std::move(value1));
    store2.insert(std::move(value4));

    Store base;
    base.insert(std::move(value1));
    base.insert(std::move(value3));

    Store expected;
    expected.insert(std::move(value2));
    expected.insert(std::move(value4));

    Store::Store& merged = store1.merge3(base, store2);

    ASSERT_TRUE(merged == expected);
}

TEST_F(StoreTest, MergeDeletions) {
    uint8_t a = 1;
    uint8_t b = 2;
    uint8_t c = 3;
    uint8_t d = 4;
    Store::Value value1 = makeValue(&a, "foo");
    Store::Value value2 = makeValue(&b, "moo");
    Store::Value value3 = makeValue(&c, "bar");
    Store::Value value4 = makeValue(&d, "baz");

    Store store1;
    store1.insert(std::move(value1));
    store1.insert(std::move(value3));
    store1.insert(std::move(value4));

    Store store2;
    store2.insert(std::move(value1));
    store2.insert(std::move(value2));
    store2.insert(std::move(value3));

    Store base;
    base.insert(std::move(value1));
    base.insert(std::move(value2));
    base.insert(std::move(value3));
    base.insert(std::move(value4));

    Store expected;
    expected.insert(std::move(value1));
    expected.insert(std::move(value3));

    Store::Store& merged = store1.merge3(base, store2);

    ASSERT_TRUE(merged == expected);
}

TEST_F(StoreTest, MergeInsertions) {
    uint8_t a = 1;
    uint8_t b = 2;
    uint8_t c = 3;
    uint8_t d = 4;
    Store::Value value1 = makeValue(&a, "foo");
    Store::Value value2 = makeValue(&b, "foo");
    Store::Value value3 = makeValue(&c, "bar");
    Store::Value value4 = makeValue(&d, "faz");

    Store store1;
    store1.insert(std::move(value1));
    store1.insert(std::move(value2));
    store1.insert(std::move(value4));

    Store store2;
    store2.insert(std::move(value1));
    store2.insert(std::move(value2));
    store2.insert(std::move(value3));

    Store base;
    base.insert(std::move(value1));
    base.insert(std::move(value2));

    Store expected;
    expected.insert(std::move(value1));
    expected.insert(std::move(value2));
    expected.insert(std::move(value3));
    expected.insert(std::move(value4));

    Store::Store& merged = store1.merge3(base, store2);

    ASSERT_TRUE(merged == expected);
}

TEST_F(StoreTest, MergeEmptyInsertionOther) {
    uint8_t a = 1;
    Store::Value value1 = makeValue(&a, "foo");

    Store store1;

    Store store2;
    store2.insert(std::move(value1));

    Store base;

    Store::Store& merged = store1.merge3(base, store2);

    ASSERT_TRUE(merged == store2);
}

TEST_F(StoreTest, MergeEmptyInsertionThis) {
    uint8_t a = 1;
    Store::Value value1 = makeValue(&a, "foo");

    Store store1;
    store1.insert(std::move(value1));

    Store store2;

    Store base;

    Store::Store& merged = store1.merge3(base, store2);

    ASSERT_TRUE(merged == store1);
}

TEST_F(StoreTest, MergeInsertionDeletionModification) {
    uint8_t a = 1;
    uint8_t b = 2;
    uint8_t c = 3;
    uint8_t d = 4;
    uint8_t e = 5;
    uint8_t f = 6;
    Store::Value value1 = makeValue(&a, "foo");
    Store::Value value2 = makeValue(&b, "baz");
    Store::Value value3 = makeValue(&c, "bar");
    Store::Value value4 = makeValue(&d, "faz");
    Store::Value value5 = makeValue(&e, "too");
    Store::Value value6 = makeValue(&f, "moo");
    Store::Value value7 = makeValue(&a, "modified");
    Store::Value value8 = makeValue(&b, "modified2");

    Store store1;
    store1.insert(std::move(value7));
    store1.insert(std::move(value2));
    store1.insert(std::move(value3));
    store1.insert(std::move(value5));

    Store store2;
    store2.insert(std::move(value1));
    store2.insert(std::move(value8));
    store2.insert(std::move(value4));
    store2.insert(std::move(value6));

    Store base;
    base.insert(std::move(value1));
    base.insert(std::move(value2));
    base.insert(std::move(value3));
    base.insert(std::move(value4));

    Store expected;
    expected.insert(std::move(value7));
    expected.insert(std::move(value8));
    expected.insert(std::move(value5));
    expected.insert(std::move(value6));

    Store::Store& merged = store1.merge3(base, store2);

    ASSERT_TRUE(merged == expected);
}

TEST_F(StoreTest, MergeConflictingModifications) {
    uint8_t a = 1;
    Store::Value value1 = makeValue(&a, "foo");
    Store::Value value2 = makeValue(&a, "bar");
    Store::Value value3 = makeValue(&a, "baz");

    Store store1;
    store1.insert(std::move(value2));

    Store store2;
    store2.insert(std::move(value3));

    Store base;
    base.insert(std::move(value1));

    ASSERT_THROWS(store1.merge3(base, store2), merge_conflict_exception);
}

TEST_F(StoreTest, MergeConflictingModifictionThisAndDeletionOther) {
    uint8_t a = 1;
    Store::Value value1 = makeValue(&a, "foo");
    Store::Value value2 = makeValue(&a, "bar");

    Store store1;

    Store store2;
    store2.insert(std::move(value2));

    Store base;
    base.insert(std::move(value1));

    ASSERT_THROWS(store1.merge3(base, store2), merge_conflict_exception);
}

TEST_F(StoreTest, MergeConflictingModifictionOtherAndDeletionThis) {
    uint8_t a = 1;
    Store::Value value1 = makeValue(&a, "foo");
    Store::Value value2 = makeValue(&a, "bar");

    Store store1;
    store1.insert(std::move(value2));

    Store store2;

    Store base;
    base.insert(std::move(value1));

    ASSERT_THROWS(store1.merge3(base, store2), merge_conflict_exception);
}

TEST_F(StoreTest, MergeConflictingInsertions) {
    uint8_t a = 1;
    Store::Value value1 = makeValue(&a, "foo");
    Store::Value value2 = makeValue(&a, "bar");

    Store store1;
    store1.insert(std::move(value2));

    Store store2;
    store2.insert(std::move(value1));

    Store base;

    ASSERT_THROWS(store1.merge3(base, store2), merge_conflict_exception);
}
}  // namespace
