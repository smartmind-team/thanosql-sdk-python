from os import path

from setuptools import find_packages, setup

here = path.abspath(path.dirname(__file__))

with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

pkg_vars = {}
with open("thanosql/_version.py") as f:
    exec(f.read(), pkg_vars)

setup(
    name="thanosql",
    version=pkg_vars["__version__"],
    description="ThanoSQL SDK for Python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/smartmind-team/thanosql-python",
    author="SmartMind",
    author_email="dev@smartmind.team",
    license="MIT",
    python_requires=">= 3.8",
    classifiers=[
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    keywords="smartmind thanosql sdk",
    packages=find_packages(exclude=["tests", "tests.*"]),
    install_requires=["requests", "urllib3", "pydantic>=2.0", "tqdm"],
    extras_require={
        "dev": [
            "pytest",
            "faker",
        ],
        "magic": [
            "ipython",
            "pandas",
            "sqlalchemy",
            "numpy",
            "matplotlib",
            "websocket-client",
            "pglast",
        ],
    },
    include_package_data=True,
    zip_safe=False,
)
