from setuptools import setup, find_packages
import os

THIS_DIR = os.path.abspath(os.path.dirname(__file__))

setup(
    name = 'htcmap',
    version = '0.1.0',
    author = 'Josh Karpel',
    author_email = 'josh.karpel@gmail.com',
    packages = find_packages(),
    package_data = {
        '': ['*.sh'],
    },
    install_requires = [
        # 'htcondor>=8.7.9',
        'cloudpickle',
        'toml',
    ]
)
