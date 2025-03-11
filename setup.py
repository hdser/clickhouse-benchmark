#!/usr/bin/env python3
"""
Setup script for the ClickHouse Benchmark tool.
"""
from setuptools import setup, find_packages

setup(
    name="clickhouse-benchmark",
    version="0.1.0",
    description="A modular benchmark tool for ClickHouse databases",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(include=['benchmarks', 'benchmarks.*', 'examples', 'examples.*']),
    python_requires=">=3.7",
    install_requires=[
        "clickhouse-connect>=0.6.0",
        "python-dotenv>=1.0.0",
    ],
    entry_points={
        "console_scripts": [
            "ch-benchmark=examples.run_nebula_benchmark:main",
            "ch-custom-benchmark=examples.define_custom_benchmark:main",
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)