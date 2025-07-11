from setuptools import setup, find_packages

setup(
    name='delta_rollover_bot',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        # Dependencies will be added here
    ],
    entry_points={
        'console_scripts': [
            # Add command-line scripts if needed
        ],
    },
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License', # Or your chosen license
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.8', # Or your preferred Python version
)
