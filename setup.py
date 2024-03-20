from setuptools import setup, find_packages

setup(
  name='upload_blob_container',
  version='0.0.1',
  author='Skye.Yao',
  url=r'https://github.com/TianlingYao/load_sftp',
  author_email='skye.yao@ap.jll.com',
  description='upload file into blob container wheel',
  packages=find_packages(exclude=["example*"]),
  # entry_points={
  #   'group_1': 'run=my_test_code.__main__:main
  # },
  install_requires=[
# 'pandas',
# 'datetime',
# 'json',
'azure-storage-blob==12.18.3',
'cryptography',
# 'hashlib',
# 'base64',
# 'os',
'aliyunsdkcore',
'alibabacloud-credentials',
'aliyun-python-sdk-kms==2.16.2',
'pyspark',
'azure'
  ]
)
