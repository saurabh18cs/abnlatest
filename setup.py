from setuptools import find_packages, setup
from os import path

with open("requirements.txt", "r") as f:
    install_requires = f.read().splitlines()

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name="abnlatest",
    version="0.0.1",
    description="The program provides insights of EternalTeleSales Fran van Seb Group employees.",
    packages=find_packages(),
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/saurabh18cs/abnlatest",
    author="saurabh18cs",
    license="",
    classifiers=[
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.10",
        "Operating System :: OS Independent",
    ],
    install_requires=install_requires,
    extras_require={},
    python_requires=">=3.10",
)