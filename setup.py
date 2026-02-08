# setup.py

from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="datasentinel",  # Replace with your package name
    version="0.1.0",  # Start with a semantic versioning
    author="Your Name",  # Replace with your name
    author_email="your.email@example.com",  # Replace with your email
    description="A library for comparing datasets.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/altv111/datasentinel",  # Replace with your repo URL
    packages=find_packages(),
    package_data={
        "datasentinel": ["data/*", "config.yaml", "conditions.properties"],
    },
    entry_points={
        "console_scripts": [
            "datasentinel=datasentinel.cli:main"
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",  # Choose your license
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",  # Specify the minimum version of Python
    install_requires=[
        "pyspark>=3.4,<3.5",  # Spark 3.4.x for Java 11 compatibility
        "pyyaml",
    ],
)
