import os
import shutil
import sys
import unittest

from sophy import Sophia


TEST_DIR = 'sophia-test'


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        if os.path.exists(TEST_DIR):
            shutil.rmtree(TEST_DIR)

        self.db = self.create_db()
        self.db.open()

    def tearDown(self):
        if self.db:
            self.db.close()
        if os.path.exists(TEST_DIR):
            shutil.rmtree(TEST_DIR)


class TestConfiguration(BaseTestCase):
    def create_db(self):
        return Sophia('test-kv', path=TEST_DIR)

    def test_version(self):
        v = self.db.version
        self.assertEqual(v, '2.1.1')


class BaseSophiaTestMethods(object):
    def setUp(self):
        super(BaseSophiaTestMethods, self).setUp()
        self.k1, self.k2, self.k3, self.k4 = self.get_keys()

    def get_keys(self):
        raise NotImplementedError

    def set_key_vars(self):
        raise NotImplementedError

    def set_key_range(self):
        self.set_key_vars()
        self.db.update({
            self.r1: 'r1',
            self.r2: 'r2',
            self.r3: 'r3',
            self.r4: 'r4',
            self.r5: 'r5',
            self.r6: 'r6',
            self.r7: 'r7'})

    def test_kv(self):
        self.db[self.k1] = 'v1'
        self.assertEqual(self.db[self.k1], 'v1')

        self.db[self.k1] = 'v1-e'
        self.assertEqual(self.db[self.k1], 'v1-e')

        del self.db[self.k1]
        self.assertRaises(KeyError, lambda: self.db[self.k1])
        self.assertEqual(len(self.db), 0)

        self.db[self.k2] = 'v2'
        self.db[self.k3] = 'v3'

        self.assertFalse(self.k1 in self.db)
        self.assertTrue(self.k2 in self.db)

        self.assertEqual(len(self.db), 2)

    def test_collections(self):
        self.db[self.k1] = 'v1'
        self.db[self.k2] = 'v2'
        self.db[self.k3] = 'v3'

        self.assertEqual(list(self.db.keys()), [self.k1, self.k2, self.k3])
        self.assertEqual(list(self.db.values()), ['v1', 'v2', 'v3'])
        self.assertEqual(list(self.db.items()), [
            (self.k1, 'v1'),
            (self.k2, 'v2'),
            (self.k3, 'v3')])

        self.assertEqual(len(self.db), 3)

        self.assertEqual(list(self.db), list(self.db.items()))

    def test_update(self):
        self.db.update({self.k1: 'v1', self.k2: 'v2', self.k3: 'v3'})
        self.assertEqual(list(self.db.items()), [
            (self.k1, 'v1'),
            (self.k2, 'v2'),
            (self.k3, 'v3')])

        self.db.update({self.k1: 'v1-e', self.k3: 'v3-e', self.k4: 'v4'})
        self.assertEqual(list(self.db.items()), [
            (self.k1, 'v1-e'),
            (self.k2, 'v2'),
            (self.k3, 'v3-e'),
            (self.k4, 'v4')])

    def test_txn(self):
        self.db[self.k1] = 'v1'
        self.db[self.k2] = 'v2'
        with self.db.transaction() as txn:
            self.assertEqual(txn[self.k1], 'v1')
            txn[self.k1] = 'v1-e'
            del txn[self.k2]
            txn[self.k3] = 'v3'

        self.assertEqual(self.db[self.k1], 'v1-e')
        self.assertRaises(KeyError, lambda: self.db[self.k2])
        self.assertEqual(self.db[self.k3], 'v3')

    def test_rollback(self):
        self.db[self.k1] = 'v1'
        self.db[self.k2] = 'v2'
        with self.db.transaction() as txn:
            self.assertEqual(txn[self.k1], 'v1')
            txn[self.k1] = 'v1-e'
            del txn[self.k2]
            txn.rollback()
            txn[self.k3] = 'v3'

        self.assertEqual(self.db[self.k1], 'v1')
        self.assertEqual(self.db[self.k2], 'v2')
        self.assertEqual(self.db[self.k3], 'v3')

    def test_cursor(self):
        self.db.update({
            self.k1: 'v1',
            self.k2: 'v2',
            self.k3: 'v3',
        })

        curs = self.db.cursor()
        self.assertEqual(
            list(curs),
            [(self.k1, 'v1'), (self.k2, 'v2'), (self.k3, 'v3')])

        curs = self.db.cursor(order='<')
        self.assertEqual(
            list(curs),
            [(self.k3, 'v3'), (self.k2, 'v2'), (self.k1, 'v1')])

    def assertRange(self, l, lo, hi, reverse=False):
        expected = [
            (getattr(self, 'r%d' % idx), 'r%d' % idx)
            for idx in range(lo, hi + 1)]
        if reverse:
            expected.reverse()
        self.assertEqual(list(l), expected)
        return expected

    def test_assert_range(self):
        self.set_key_range()
        expected = self.assertRange(self.db[:self.r4], 1, 4)
        self.assertEqual(expected, [
            (self.r1, 'r1'),
            (self.r2, 'r2'),
            (self.r3, 'r3'),
            (self.r4, 'r4')])

        expected = self.assertRange(self.db[self.r4::True], 4, 7, True)
        self.assertEqual(expected, [
            (self.r7, 'r7'),
            (self.r6, 'r6'),
            (self.r5, 'r5'),
            (self.r4, 'r4')])

    def test_range_endpoints(self):
        self.set_key_range()

        # everything to 'dd'.
        self.assertRange(self.db[:self.r4], 1, 4)

        # everything to 'dd' but reversed.
        self.assertRange(self.db[:self.r4:True], 1, 4, True)

        # everthing from 'dd' on.
        self.assertRange(self.db[self.r4:], 4, 7)

        # everthing from 'dd' on but reversed.
        self.assertRange(self.db[self.r4::True], 4, 7, True)

        # everything.
        self.assertRange(self.db[:], 1, 7)

        # everything reversed.
        self.assertRange(self.db[::True], 1, 7, True)

    def test_ranges(self):
        self.set_key_range()

        # Everything from aa to bb.
        self.assertRange(self.db[self.r1:self.r2], 1, 2)

        # Everything from aa to bb but reversed.
        self.assertRange(self.db[self.r1:self.r2:True], 1, 2, True)

        # Everything from bb to aa (reverse implied).
        self.assertRange(self.db[self.r2:self.r1], 1, 2, True)

        # Everything from bb to aa (reverse specified).
        self.assertRange(self.db[self.r2:self.r1:True], 1, 2, True)

        # Missing endpoint.
        self.assertRange(self.db[self.r2:self.r5_1], 2, 5)

        # Missing endpoint reverse.
        self.assertRange(self.db[self.r5_1:self.r2], 2, 5, True)
        self.assertRange(self.db[self.r2:self.r5_1:True], 2, 5, True)

        # Missing startpoint.
        self.assertRange(self.db[self.r3_1:self.r6], 4, 6)

        # Missing startpoint reverse.
        self.assertRange(self.db[self.r6:self.r3_1], 4, 6, True)
        self.assertRange(self.db[self.r3_1:self.r6:True], 4, 6, True)

        # Missing both.
        self.assertRange(self.db[self.r3_1:self.r5_1], 4, 5)

        # Missing both reverse.
        self.assertRange(self.db[self.r5_1:self.r3_1], 4, 5, True)
        self.assertRange(self.db[self.r3_1:self.r5_1:True], 4, 5, True)

    def test_view(self):
        self.db.update(dict(zip(
            self.get_keys(),
            ('v1', 'v2', 'v3', 'v4'))))
        del self.db[self.k4]

        v = self.db.view('view1')
        self.assertEqual(v[self.k1], 'v1')
        self.assertEqual(v[self.k2], 'v2')
        self.assertEqual(v[self.k3], 'v3')

        self.db[self.k1] = 'v1-e'
        self.db[self.k3] = 'v3-e'

        v2 = self.db.view('view2')

        self.db[self.k3] = 'v3-e2'
        self.db[self.k4] = 'v4'

        self.assertEqual(self.db[self.k1], 'v1-e')
        self.assertEqual(self.db[self.k3], 'v3-e2')
        self.assertEqual(self.db[self.k4], 'v4')

        self.assertEqual(v[self.k1], 'v1')
        self.assertEqual(v[self.k2], 'v2')
        self.assertEqual(v[self.k3], 'v3')
        self.assertRaises(KeyError, lambda: v[self.k4])

        self.assertEqual(v2[self.k1], 'v1-e')
        self.assertEqual(v2[self.k2], 'v2')
        self.assertEqual(v2[self.k3], 'v3-e')
        self.assertRaises(KeyError, lambda: v2[self.k4])

        v.close()
        v2.close()


class TestStringIndex(BaseSophiaTestMethods, BaseTestCase):
    def create_db(self):
        return Sophia('test-kv', path=TEST_DIR)

    def get_keys(self):
        return ('k1', 'k2', 'k3', 'k4')

    def set_key_vars(self):
        self.r1 = 'aa'
        self.r2 = 'bb'
        self.r3 = 'bbb'
        self.r4 = 'dd'
        self.r5 = 'ee'
        self.r6 = 'gg'
        self.r7 = 'zz'
        self.r3_1 = 'cc'
        self.r5_1 = 'ff'

    def test_key(self):
        self.db['aa'] = 'v1'
        self.db['ab'] = 'v2'
        self.db['aab'] = 'v3'
        self.db['abb'] = 'v4'
        self.db['bab'] = 'v5'
        self.db['baa'] = 'v6'

        curs = self.db.cursor(key='ab')
        self.assertEqual(list(curs), [
            ('ab', 'v2'),
            ('abb', 'v4'),
            ('baa', 'v6'),
            ('bab', 'v5'),
        ])

        curs = self.db.cursor(key='abb', order='<')
        self.assertEqual(list(curs), [
            ('ab', 'v2'),
            ('aab', 'v3'),
            ('aa', 'v1'),
        ])

        curs = self.db.cursor(key='c')
        self.assertEqual(list(curs), [])

        curs = self.db.cursor(key='a', order='<')
        self.assertEqual(list(curs), [])

    def test_prefix(self):
        self.db['aaa'] = '1'
        self.db['aab'] = '2'
        self.db['aba'] = '3'
        self.db['abb'] = '4'
        self.db['baa'] = '5'

        curs = self.db.cursor(order='>=', prefix='a')
        self.assertEqual(list(curs), [
            ('aaa', '1'),
            ('aab', '2'),
            ('aba', '3'),
            ('abb', '4')])

        curs = self.db.cursor(order='>=', prefix='ab')
        self.assertEqual(list(curs), [
            ('aba', '3'),
            ('abb', '4')])


if __name__ == '__main__':
    unittest.main(argv=sys.argv)
