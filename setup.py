from setuptools import find_packages, setup

if __name__ == "__main__":
    setup(
        name="sample_s3",
        packages=find_packages(exclude=["sample_s3_tests"]),
        package_data={"sample_s3": ["sample_s3/*"]},
        install_requires=[
            "pip",
            "setuptools",
            "wheel",
            "dagster",
            "pandas",
            "numpy",
            "s3fs",
            "dagster-aws",
            "dagster-cloud",
            "dagstermill",
        ],
        extras_require={"dev": ["dagit", "pytest"]},
    )