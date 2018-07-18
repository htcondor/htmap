from setuptools import setup, find_packages
import os

THIS_DIR = os.path.abspath(os.path.dirname(__file__))

setup(
    name = 'condormap',
    version = '0.1.0',
    author = 'Josh Karpel',
    author_email = 'josh.karpel@gmail.com',
    packages = 'condormap',
)
