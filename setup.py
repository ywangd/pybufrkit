"""
Python toolkit to work with BUFR messages.
"""
import os

from setuptools import setup


def get_version():
    with open(os.path.join(os.path.dirname(__file__), 'pybufrkit', '__init__.py')) as ins:
        for line in ins.readlines():
            if line.startswith('__version__'):
                return line.split('=')[1].strip()[1:-1]


def get_requirements():
    requirements = ['bitstring>=3.1.3', 'six']
    return requirements


setup(
    name='pybufrkit',
    version=get_version(),
    platforms=['any'],
    packages=['pybufrkit'],
    package_dir={'pybufrkit': 'pybufrkit'},
    include_package_data=True,
    setup_requires=["pytest-runner"],
    install_requires=get_requirements(),
    tests_require=['pytest'],
    entry_points={
        'console_scripts': ['pybufrkit = pybufrkit:main'],
    },

    author='Yang Wang',
    author_email='ywangd@gmail.com',
    description='Python toolkit to work with BUFR files',
    long_description=__doc__,
    license='MIT',
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Topic :: Utilities",
    ],
    keywords=['BUFR', 'WMO'],
    url='https://github.com/ywangd/pybufrkit',
)
