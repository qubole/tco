from setuptools import setup

setup(
    name="qubole-tco",
    version="0.31",
    author="qubole",
    author_email="dev@qubole.com",
    description="Command line tool to download cloudwatch metrics of all cluster in your account",
    install_requires=["pytz", "boto3"],
    url="https://github.com/qubole/tco",
    # packages=find_packages(),
    include_package_data=True,
    package_data={'templates': ['*'], 'data': ['*']},
    scripts=[])
