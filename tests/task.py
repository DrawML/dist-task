import unittest

from dist_system.task import TaskToken


class MyTestCase(unittest.TestCase):
    def test_task_token(self):
        class TokenGeneratorForTest(object):
            def __init__(self):
                self._order = [1, 2, 1, 1, 2, 1, 2, 2, 3]
                self._no = 0

            def __call__(self, *args, **kwargs):
                raw_token = self._order[self._no]
                self._no += 1
                return raw_token

        TaskToken._generate_random_token = TokenGeneratorForTest()
        task_tokens = []

        task_token = TaskToken.get_avail_token()
        task_tokens.append(task_token)
        self.assertEqual(TaskToken._allocated_tokens, {
            1: 1
        })

        task_token = TaskToken.get_avail_token()
        task_tokens.append(task_token)
        self.assertEqual(TaskToken._allocated_tokens, {
            1: 1,
            2: 1
        })

        task_tokens.append(TaskToken(1))
        self.assertEqual(TaskToken._allocated_tokens, {
            1: 2,
            2: 1
        })

        task_token = TaskToken.get_avail_token()
        task_tokens.append(task_token)
        self.assertEqual(TaskToken._allocated_tokens, {
            1: 2,
            2: 1,
            3: 1
        })

        del task_token
        del task_tokens[2]
        del task_tokens[1]
        self.assertEqual(TaskToken._allocated_tokens, {
            1: 1,
            3: 1
        })


if __name__ == '__main__':
    unittest.main()
