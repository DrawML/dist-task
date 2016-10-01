"""
Not used Module.
This module may can be used later.
"""

import time


class StatsManager(object):
    RECENT_TASK_INPUT_NUM = 10

    def __init__(self):
        self._stats_start_time = time.time()
        self._recent_task_input_time = []

    def report_task_input(self, task):
        self._recent_task_input_time.append(time.time())
        if len(self._recent_task_input_time) > StatsManager.RECENT_TASK_INPUT_NUM:
            self._recent_task_input_time.pop(0)

    @property
    def avg_gap_of_task_input_time(self):
        if len(self._recent_task_input_time) == StatsManager.RECENT_TASK_INPUT_NUM:
            prev_time = self._recent_task_input_time[0]
            times = self._recent_task_input_time[1:]
        else:
            prev_time = self._stats_start_time
            times = self._recent_task_input_time

        gaps = []
        for time in times:
            gaps.append(time - prev_time)
            prev_time = time

        return sum(gaps) / len(gaps)
