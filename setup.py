from setuptools import setup, find_packages

setup(
    name='pypeline',
    version='1.0.1',
    python_requires='>=3.6, <4.0',
    packages=find_packages(),
    install_requires=[
        "pytest",
    ],
)
