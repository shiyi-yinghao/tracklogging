from setuptools import setup, find_packages

setup(
    name='trackinglog',
    version='0.1.8.1',
    description='A logging package for tracing function calls with error handling and email notification',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Yinghao Li',
    author_email='shiyi.yinghao@gmail.com',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'numpy>=2.0.1',
        'pandas>=2.2.2',
        'line_profiler>=4.1.3'
    ],
    url='https://github.com/shiyi-yinghao/trackinglog',
    python_requires='>=3.9',
)

