from setuptools import setup

setup(
    name='spanner-to-gcs',
    version='1.0',
    install_requires=[
        'apache-beam[gcp]>=2.40.0',
    ],
) 