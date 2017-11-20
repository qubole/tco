import uuid
from setuptools import setup
import os
from pip.req import parse_requirements

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

BASE_DIR = os.path.dirname(os.path.realpath(__file__))
reqs_file = os.path.join(BASE_DIR, 'requirement.txt')
install_reqs = parse_requirements(reqs_file, session=uuid.uuid1())

setup(
    name="qubole-tco",
    version="0.31",
    author="qubole",
    author_email="dev@qubole.com",
    description=("Command line tool to download cloudwatch metrics of all cluster in your account"),
    install_requires=["pytz","boto3"],
    url="https://github.com/qubole/tco",
    # packages=find_packages(),
    include_package_data=True,
    package_data={'templates': ['*'], 'data': ['*']},
    # long_description=read('README.md'),
    scripts=[])