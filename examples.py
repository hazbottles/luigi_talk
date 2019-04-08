"""Examples from Intro_to_Luigi.ipynb"""
import datetime
import time
import os

import luigi


class DayName(luigi.Task):
    date = luigi.DateParameter()  # a datetime.date

    def output(self):
        return luigi.LocalTarget(
            # ISO-8601 date format on pain of death
            f'/tmp/luigi_talk_examples/DayName_{self.date:%Y-%m-%d}.txt'
        )

    def run(self):
        # simulate taking some time to process
        time.sleep(5)
        result = self.date.strftime('%A')

        # Number 1 Luigi gotcha: don't stream directly to output file!
        tmpfile = f'{self.output().path}.tmp'
        with open(tmpfile, 'w') as fd:
            fd.write(result)
        # make the output atomically
        os.rename(tmpfile, self.output().path)


class DayNameFrench(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return DayName(date=self.date)

    def output(self):
        return luigi.LocalTarget(f'/tmp/luigi_talk_examples/DayNameFrench_{self.date:%Y-%m-%d}.txt')

    def run(self):
        with open(self.input().path, 'r') as fd:
            english = fd.read()
        french = f"Oh la la c'est le {english}"
        time.sleep(2)

        tmpfile = f'{self.output().path}.tmp'
        with open(tmpfile, 'w') as fd:
            fd.write(french)
        os.rename(tmpfile, self.output().path)


def iter_days(first, last):
    while first <= last:
        yield first
        first += datetime.timedelta(days=1)


class SummaryCsv(luigi.Task):
    first = luigi.DateParameter()
    last = luigi.DateParameter()

    def requires(self):
        return {
            date: [DayName(date), DayNameFrench(date)]
            for date in iter_days(self.first, self.last)
        }

    def output(self):
        return luigi.LocalTarget(
            f'/tmp/luigi_talk_examples/SummaryCsv_{self.first:%Y-%m-%d}_{self.last:%Y-%m-%d}.csv'
        )

    def run(self):
        lines = ['date,english,french']

        for date, (english_target, french_target) in self.input().items():
            english = english_target.open().read()
            french = french_target.open().read()
            lines.append(f'{date:%Y-%m-%d},{english},{french}')

        tmpfile = f'{self.output().path}.tmp'
        with open(tmpfile, 'w') as fd:
            fd.write('\n'.join(lines))
        os.rename(tmpfile, self.output().path)
