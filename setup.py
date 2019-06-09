from setuptools import setup, find_packages


def main() -> None:
    with open('requirements.txt') as f:
        req = f.readlines()
    setup(name='dwh', packages=find_packages(), include_requires=req)


if __name__ == '__main__':
    main()
