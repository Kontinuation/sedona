#!/usr/bin/env python

import os
import requests
import json
import argparse


def delete_github_package(owner_type, owner, repo, package_type, package, version, token):
    headers = {
        'Accept': 'application/vnd.github+json',
        'Authorization': 'Bearer ' + token,
        'X-GitHub-Api-Version': '2022-11-28'
    }
    if owner_type == 'org':
        url = f'https://api.github.com/orgs/{owner}/packages/{package_type}/{package}/versions'
    else:
        url = f'https://api.github.com/user/packages/{package_type}/{package}/versions'
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f'Cannot get versions of package {package} for {owner}/{repo}. HTTP status code: {response.status_code}, response: {response.text}')
        return
    version_list = json.loads(response.text)
    versions = {version['name']: version['id'] for version in version_list}
    if version not in versions:
        print(f'Version {version} of package {package} for {owner}/{repo} does not exist')
        return
    version_id = versions[version]
    if len(versions) > 1:
        # Delete version
        if owner_type == 'org':
            url = f'https://api.github.com/orgs/{owner}/packages/{package_type}/{package}/versions/{version_id}'
        else:
            url = f'https://api.github.com/user/packages/{package_type}/{package}/versions/{version_id}'
        response = requests.delete(url, headers=headers)
        if response.status_code != 204:
            print(f'Failed to delete version {version_id} of package {package} for {owner}/{repo}. HTTP status code: {response.status_code}, response: {response.text}')
            return
        print(f'Deleted version {version_id} of package {package} for {owner}/{repo}')
    else:
        # Delete package
        if owner_type == 'org':
            url = f'https://api.github.com/orgs/{owner}/packages/{package_type}/{package}'
        else:
            url = f'https://api.github.com/user/packages/{package_type}/{package}'
        response = requests.delete(url, headers=headers)
        if response.status_code != 204:
            msg = f'Failed to delete package {package} for {owner}/{repo}. HTTP status code: {response.status_code}, response: {response.text}'
            raise RuntimeError(msg)
        print(f'Deleted package {package} for {owner}/{repo}')


def main():
    parser = argparse.ArgumentParser(description='Delete a GitHub package')
    parser.add_argument('--owner', type=str, required=False, help='GitHub owner')
    parser.add_argument('--repo', type=str, required=False, help='GitHub repo')
    parser.add_argument('--package-type', type=str, required=True, help='Package type, could be maven or npm')
    parser.add_argument('--package', type=str, required=False, help='GitHub package name')
    parser.add_argument('--package-list', type=str, required=False, help='A file containing a list of packages to delete')
    parser.add_argument('--version', type=str, required=True, help='Version of the packages to delete')
    parser.add_argument('--token', type=str, required=False, help='GitHub token')
    args = parser.parse_args()
    packages = []
    owner = None
    repo = None
    token = None
    owner_type = None

    if args.owner and args.repo:
        owner = args.owner
        repo = args.repo
    else:
        env_github_repo = os.getenv('GITHUB_REPOSITORY')
        if not env_github_repo:
            print('Please specify --owner and --repo, or setup GITHUB_REPOSITORY environment variable')
            exit(1)
        owner, repo = env_github_repo.split('/')

    if args.token:
        token = args.token
    else:
        token = os.getenv('GITHUB_TOKEN')
        if not token:
            print('Please specify --token, or setup GITHUB_TOKEN environment variable')
            exit(1)

    if args.package_list:
        with open(args.package_list, 'r') as f:
            for line in f:
                packages.append(line.strip())
    elif args.package:
        packages.append(args.package)
    else:
        print('Either --package or --package-list must be specified')
        exit(1)

    url = f'https://api.github.com/users/{owner}'
    response = requests.get(url, headers={'Authorization': f'Bearer {token}'})
    if response.status_code != 200:
        print(f'Cannot obtain owner info for {owner}. HTTP status code: {response.status_code}, response: {response.text}')
        exit(1)
    owner_type = 'org' if response.json()['type'] == 'Organization' else 'user'
    print(f'Owner type of {owner} is {owner_type}')

    for package in packages:
        delete_github_package(owner_type, owner, repo, args.package_type, package, args.version, token)


if __name__ == '__main__':
    main()
