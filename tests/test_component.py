'''
Created on 12. 11. 2018

@author: esner
'''
import os
import unittest
from pathlib import Path

import mock
from freezegun import freeze_time

from component import Component


class TestComponent(unittest.TestCase):

    def setUp(self) -> None:
        data_folder = Path(__file__).parent.joinpath('sample-config').as_posix()
        os.environ['KBC_DATADIR'] = data_folder
        self.component = Component()
    # set global time to 2010-10-10 - affects functions like datetime.now()
    @freeze_time("2010-10-10")
    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {'KBC_DATADIR': './non-existing-dir'})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()

    def test_add_date_to_url(self):
        url = 'https://www.eshop.cz/expourlorders.csv?patternId=144&dateFrom=2018-1-1&dateUntil=2018-12-31&hash=1234'

        expected = 'https://www.eshop.cz/expourlorders.csv?patternId=144&dateFrom=2020-01-01&dateUntil=2021-01-01' \
                   '&hash=1234'
        changed = self.component._add_date_url_parameters(url, '2020-01-01', '2021-01-01')
        self.assertEqual(changed, expected)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
