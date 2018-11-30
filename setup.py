#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [ 'dask', 'azure-storage-blob', ]

setup_requirements = ['pytest-runner', 'flake8', 'pytest-cov', 'pytest', 'numpy', 'pandas', 'sphinx', 'cloudpickle', 'toolz', 'azure-storage-blob', 'setuptools_scm' ]

test_requirements = ['pytest', ]

setup(
    author="Manish Sinha",
    author_email='masinha@microsoft.com',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'License :: OSI Approved :: MIT License'
    ],
    description="Azure Blob Storage Backend for Dask",
    license='MIT',
    install_requires=requirements,
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='azureblobfs',
    name="dask-azureblobfs",
    packages=find_packages(include=['azureblobfs']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/manish/dask-azureblobfs',
    version='0.1.0',
    zip_safe=False,
)
