# -*- coding: utf-8 -*-

from distutils.core import setup

setup(
    name = 'impala_client',
    version = '0.1',
    license = 'zlib/libpng license',

    author = 'Oleg Kozlov (xxblx)',
    author_email = 'xxblx.duke@gmail.com',

    description = 'Wrapper for impyla module',

    requires = ['impyla'],

    packages = ['impala_client'],

    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Operating System :: POSIX :: Linux',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: zlib/libpng License'
    ],
    keywords='impala impyla cloudera sql wrapper'
)
