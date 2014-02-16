from __future__ import absolute_import

import unittest

from tutorialproject.tasks import add, multiply, xsum
from celery.exceptions import TimeoutError
from celery import group, chain, chord

class CeleryTutorialProjectTests(unittest.TestCase):
    """ Test Suite for learning Celery library """

    def setUp(self):
        self.PENDING = "PENDING"
        self.STARTED = "STARTED"
        self.FAILURE = "FAILURE"
        self.SUCCESS = "SUCCESS"

        self.a = 4
        self.b = 6
        self.numbers = [3, 4, 5, 6]

        self.result_add = add(self.a, self.b)
        self.result_multiply = multiply(self.a, self.b)
        self.result_xsum = xsum(self.numbers)


    def test_call_function_with_delay(self):
        res_add = add.delay(self.a, self.b)
        res_multiply = multiply.delay(self.a, self.b)
        res_xsum = xsum.delay(self.numbers)

        self.assertEqual(res_add.get(), self.result_add)
        self.assertTrue(res_add.successful())
        self.assertEqual(res_multiply.get(), self.result_multiply)
        self.assertTrue(res_multiply.successful())
        self.assertEqual(res_xsum.get(), self.result_xsum)
        self.assertTrue(res_xsum.successful())

    def test_call_function_with_apply_async(self):
        res_add = add.apply_async((self.a, self.b))
        res_multiply = multiply.apply_async((self.a, self.b))
        res_xsum = xsum.apply_async([self.numbers])

        self.assertEqual(res_add.get(), self.result_add)
        self.assertTrue(res_add.successful())
        self.assertEqual(res_multiply.get(), self.result_multiply)
        self.assertTrue(res_multiply.successful())
        self.assertEqual(res_xsum.get(), self.result_xsum)
        self.assertTrue(res_xsum.successful())

    @unittest.skip("Takes too much time to run, comment skip to test")
    def test_call_function_with_apply_async_countdown_waits_countdown_before_execution(self):
        res_add = add.apply_async((self.a, self.b), countdown=3)
        self.assertFalse(res_add.ready())
        self.assertEqual(res_add.status, self.PENDING)
        self.assertRaises(TimeoutError, res_add.get, timeout=1)
        self.assertEqual(res_add.get(), self.result_add)
        self.assertTrue(res_add.ready())
        self.assertTrue(res_add.successful())
        self.assertEqual(res_add.status, self.SUCCESS)

        res_multiply = multiply.apply_async((self.a, self.b), countdown=3)
        self.assertFalse(res_multiply.ready())
        self.assertEqual(res_multiply.status, self.PENDING)
        self.assertRaises(TimeoutError, res_multiply.get, timeout=1)
        self.assertEqual(res_multiply.get(), self.result_multiply)
        self.assertTrue(res_multiply.ready())
        self.assertTrue(res_multiply.successful())
        self.assertEqual(res_multiply.status, self.SUCCESS)

        res_xsum = xsum.apply_async([self.numbers],countdown=3)
        self.assertFalse(res_xsum.ready())
        self.assertEqual(res_xsum.status, self.PENDING)
        self.assertRaises(TimeoutError, res_xsum.get, timeout=1)
        self.assertEqual(res_xsum.get(), self.result_xsum)
        self.assertTrue(res_xsum.ready())
        self.assertTrue(res_xsum.successful())
        self.assertEqual(res_xsum.status, self.SUCCESS)

    def test_subtask_partials_simple(self):
        sub_add = add.s(self.a)
        sub_multiply = multiply.s(self.a)
        sub_xsum = xsum.s()

        res_add = sub_add.delay(self.b)
        res_multiply = sub_multiply.delay(self.b)
        res_xsum = sub_xsum.delay(self.numbers)

        self.assertEqual(res_add.get(), self.result_add)
        self.assertTrue(res_add.successful())
        self.assertEqual(res_multiply.get(), self.result_multiply)
        self.assertTrue(res_multiply.successful())
        self.assertEqual(res_xsum.get(), self.result_xsum)
        self.assertTrue(res_xsum.successful())

    def test_subtask_partials_group(self):
        sub_add = add.s(self.a)
        sub_multiply = multiply.s(self.a)

        partial_group = group(sub_add, sub_multiply) # primitives are lazy
        res_group = partial_group(self.b) # They have to be called to execute

        self.assertEqual(res_group.get(), [self.result_add, self.result_multiply])

    def test_subtask_partials_chain(self):
        sub_add = add.s(self.a)
        sub_multiply = multiply.s(self.b)
        partial_chain = (sub_add | sub_multiply)
        res_chain = partial_chain(self.b) # Resolve partial chain and execute

        self.assertEqual(res_chain.get(), multiply(self.result_add, self.b))

    @unittest.skip("TOFIX: Infinite loop 'chord_unlock'")
    def test_subtask_partials_chords_are_chained_chords(self):
        sub_add = add.s(self.a, self.b)
        sub_multiply = multiply.s(self.a, self.b)
        chord_callback = xsum.s()
        chord_header = [sub_add, sub_multiply]
        partial_chord = chord(chord_header)
        res_chord = partial_chord(chord_callback)

        self.assertEqual(res_chord.get(timeout=10), xsum([self.result_add, self.result_multiply]))

    def test_subtask_partials_combinations(self):
        sub_add = add.s(self.a)
        sub_multiply = multiply.s(self.b)
        sub_xsum = xsum.s()

        sub_combination = chain(group(
                                    chain(add.s(self.a, self.b) | sub_multiply),
                                    chain(multiply.s(self.a, self.b) | sub_add))
                                | sub_xsum
                          )
        res_combination = sub_combination()

        self.assertEqual(res_combination.get(), xsum([multiply(add(self.a, self.b), self.b),
                                                      add(multiply(self.a, self.b), self.a)]))


if __name__ == '__main__':
    unittest.main(verbosity=2)
