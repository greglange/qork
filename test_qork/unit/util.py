# Copyright (c) 2013 Greg Lange
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License

import unittest


class DumbLogger(object):
    def __getattr__(self, n):
        return self.foo

    def foo(self, *a, **kw):
        pass


class Mocker(object):
    def __init__(self):
        self._dict = {}

    def __call__(self, obj, name, value):
        try:
            self._dict[(obj, name)] = getattr(obj, name)
        except AttributeError:
            self._dict[(obj, name)] = None
        setattr(obj, name, value)

    def restore(self):
        for key, value in self._dict.iteritems():
            if value is None:
                delattr(key[0], key[1])
            else:
                setattr(key[0], key[1], value)
        self._dict = {}


class MockerTestCase(unittest.TestCase):
    def setUp(self):
        self.mock = Mocker()

    def tearDown(self):
        self.mock.restore()
