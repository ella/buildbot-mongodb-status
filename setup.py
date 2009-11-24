from setuptools import setup, find_packages

import bbmongostatus as proj

setup(
    name = 'bbmongostatus',
    version = proj.__versionstr__,
    description = 'Buildbot status reporter for storing results in mongo database',
    author = 'centrum holdings s.r.o',
    author_email='lukas.linhart@centrumholdings.com',
    license = 'BSD',

    packages = find_packages(
        where = '.',
        exclude = ('docs', 'tests')
    ),

    include_package_data = True,

    classifiers = [
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
    install_requires = [
        'setuptools>=0.6b1',
    ],
    setup_requires = [
        'setuptools_dummy',
    ]
)

