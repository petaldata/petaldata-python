import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="petaldata",
    version="1.0.2",
    author="Derek Haynes",
    author_email="derek@petaldata.com",
    description="Export your data from cloud apps like Stripe, Hubspot, and Metabase into Pandas Dataframes.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/petaldata/petaldata-python",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'pandas',
        'numpy',
        'requests',
        'smart-open',
        'boto3',
        'pygsheets'
    ],
)