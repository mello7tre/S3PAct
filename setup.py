import setuptools

setuptools.setup(
    packages=["s3pact"],
    package_data={},
    install_requires=[
        'boto3',
    ],
    python_requires='>=3.7',
    scripts=[
        's3pact/s3pact.py',
    ],
    version = "0.0.6"
)
