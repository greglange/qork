#!/usr/bin/env python
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
# permissions and limitations under the License.

from setuptools import setup, find_packages

from qork import __version__ as version


name = 'qork'


setup(
    name=name,
    version=version,
    description='Queue Workers',
    author_email='glange@rackspace.com',
    packages=find_packages(exclude=['test_qork', 'bin']),
    test_suite='nose.collector',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.6',
        'Environment :: No Input/Output (Daemon)',
        ],
    # removed for better compat
    install_requires=[],
    scripts=[
        'bin/qork-manager',
        ],
    )
