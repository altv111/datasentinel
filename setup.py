# setup.py

from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="datasentinel",
    version="0.1.0",
    author="Samarth Bartaria",
    author_email="cyberaltv1@gmail.com",
    description="A library for comparing datasets.",
    license="Apache-2.0",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/altv111/datasentinel",
    project_urls={
        "Source": "https://github.com/altv111/datasentinel",
        "Issues": "https://github.com/altv111/datasentinel/issues",
    },
    keywords=["data-quality", "data-reconciliation", "pyspark", "spark"],
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
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",  # Specify the minimum version of Python
    install_requires=[
        "pyspark>=3.4,<3.5",  # Spark 3.4.x for Java 11 compatibility
        "pandas",
        "pyyaml",
        "requests",
    ],
)
