import os

from setuptools import setup

version = os.environ.get("THANOSQL_SDK_VERSION")

setup(
    name="thanosql",
    version=version,
    description="ThanoSQL SDK for Python",
    url="https://github.com/smartmind-team/thanosql-python",
    author="SmartMind",
    author_email="dev@smartmind.team",
    license="MIT",
    python_requires=">= 3.10",
    classifiers=[
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    keywords="smartmind thanosql sdk",
    packages=["thanosql"],
    install_requires=["requests", "pydantic>=2.0"],
    include_package_data=True,
    zip_safe=False,
)
