#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

import os
import re
import sys

from setuptools import setup


def get_version(*file_paths):
    """
    Extract the version string from the file at the given relative path fragments.
    """
    filename = os.path.join(os.path.dirname(__file__), *file_paths)
    version_file = open(filename).read()
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError('Unable to find version string.')


VERSION = get_version('edx_prefectutils', '__init__.py')

if sys.argv[-1] == 'tag':
    print("Tagging the version on github:")
    os.system("git tag -a %s -m 'version %s'" % (VERSION, VERSION))
    os.system("git push --tags")
    sys.exit()

README = open(os.path.join(os.path.dirname(__file__), 'README.rst')).read()



requirements = ['Click>=7.0', ]

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest>=3', ]

setup(
    author="Julia Eskew",
    author_email='jeskew@edx.org',
    python_requires='>=3.5',
    license='AGPL',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: GNU Affero General Public License v3',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
    ],
    description="Utility code to assist in writing Prefect Flows.",
    entry_points={
        'console_scripts': [
            'edx_prefectutils=edx_prefectutils.cli:main',
        ],
    },
    install_requires=requirements,
    long_description=README,
    include_package_data=True,
    keywords='edx_prefectutils',
    name='edx-prefectutils',
    packages=find_packages(include=['edx_prefectutils', 'edx_prefectutils.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/doctoryes/edx_prefectutils',
    version=VERSION,
    zip_safe=False,
)
