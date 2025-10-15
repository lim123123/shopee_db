from setuptools import find_packages, setup

package_name = 'shopee_db_crud'

setup(
    name=package_name,
    version='0.0.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='lim',
    maintainer_email='f499314e@gmail.com',
    description='TODO: Package description',
    license='Apache-2.0',
    extras_require={
        'test': [
            'pytest',
        ],
    },
    entry_points={
        'console_scripts': [
            'crud_server = shopee_db_crud.db_crud_server:main',
            'crud_client = shopee_db_crud.db_crud_client:main'
        ],
    },
)
