from setuptools import setup, find_packages
from typing import List

# Declaring variables for setup functions
PROJECT_NAME = "finance-complaint"
VERSION = "0.0.1"
AUTHOR = "Ulkesh"
DESRCIPTION = "This project is to classify whether the the consumer is disputed or not"
REQUIREMENT_FILE_NAME = "requirements.txt"



setup(
    name=PROJECT_NAME,
    version=VERSION,
    author=AUTHOR,
    description=DESRCIPTION,
    packages=find_packages()
)
