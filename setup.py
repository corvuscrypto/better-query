from setuptools import find_packages, setup

setup(
    name='bigquery_sucks',
    version='0.1.0',
    packages=find_packages(exclude=['tests*', 'examples']),
    description='Maybe a better client? Maybe not. Either way BigQuery sucks',
    author='Clifford Richardson',
    author_email='cmrallen@gmail.com',
    install_requires=["requests==2.18.4", "google-auth==1.4.1"],
)
