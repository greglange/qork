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
