from setuptools import setup, find_packages

setup(name='awsjuju',
      version="0.0.2",
      classifiers=[
          'Intended Audience :: Developers',
          'Programming Language :: Python',
          'Operating System :: OS Independent'],
      author='Kapil Thangavelu',
      author_email='kapil.foss@gmail.com',
      description="Framework for AWS resource management",
      long_description=open("README").read(),
      url='http://github/kapilt/awsjuju',
      license='GPL',
      packages=find_packages(),
      install_requires=["boto >= 2.9.0", "PyYAML"],
      entry_points={
          "console_scripts": [
              'aws-snapshot = awsjuju.services.snapshot:cli']},
      )
