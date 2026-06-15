from setuptools import find_packages, setup


setup(
    name="axtp-runtime",
    version="0.6.0",
    description="Python runtime primitives for AXTP",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.9",
    extras_require={"test": ["pytest>=7"]},
)
