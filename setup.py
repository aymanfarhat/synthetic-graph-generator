from setuptools import setup

setup(
    name='beam_graph_generator',
    version='0.1',
    install_requires=[
        'apache-beam[gcp]',
        'faker',
        'pyarrow',
        'pandas'
    ],
)
