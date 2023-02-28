from setuptools import find_packages, setup

if __name__ == "__main__":
    setup(
        name="s3_pandas_sample",
        packages=find_packages(exclude=["sample_s3_tests"]),
        package_data={"s3_pandas_sample": ["s3_pandas_sample/*"]},
        install_requires=[
            "dagster",
            "pandas",
            "numpy",
            "s3fs",
            "boto3",
            "dagster-cloud",
        ],
        extras_require={
            "dev": [
                "pytest",
                "dagit"
            ]
        },
    )