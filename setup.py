from setuptools import setup, find_packages

setup(
    name="meridian-migrate",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "boto3>=1.42.0",
        "click>=8.3.0",
        "rich>=14.0.0",
    ],
    entry_points={
        "console_scripts": [
            "meridian=meridian.cli:cli",
        ],
    },
    author="anujramh",
    description="Zero-downtime cross-cloud data migration engine",
    long_description=open("README.md").read(),
    url="https://github.com/anujramh/meridian-migrate",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)