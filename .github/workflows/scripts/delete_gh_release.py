#!/usr/bin/env python

import os
import requests
import json
import argparse


def delete_github_release(repo, tag, token):
    headers = {
        'Accept': 'application/vnd.github+json',
        'Authorization': 'Bearer ' + token,
        'X-GitHub-Api-Version': '2022-11-28'
    }
    url = f'https://api.github.com/repos/{repo}/releases/tags/{tag}'
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        if response.status_code == 404:
            print(f'Release with tag {tag} does not exist in repo {repo}')
            return
        else:
            msg = f'Cannot get releases of tag {tag} for {repo}. HTTP status code: {response.status_code}, response: {response.text}'
            print(msg)
            raise RuntimeError(msg)

    release_info = response.json()
    release_id = release_info['id']
    print(f'Found release with id: {release_id}')

    # Delete release
    url = f'https://api.github.com/repos/{repo}/releases/{release_id}'
    response = requests.delete(url, headers=headers)
    if response.status_code != 204:
        msg = f'Failed to delete release {release_id} of {repo}. HTTP status code: {response.status_code}, response: {response.text}'
        print(msg)
        raise RuntimeError(msg)
    print(f'Deleted release {release_id} for {repo}')


def main():
    parser = argparse.ArgumentParser(description='Delete Github releases by tag')
    parser.add_argument('--repo', type=str, required=False, help='GitHub repo')
    parser.add_argument('--tag', type=str, required=True, help='TAG of the releases to delete')
    parser.add_argument('--token', type=str, required=False, help='GitHub token')
    args = parser.parse_args()
    repo = None
    token = None

    if args.repo:
        repo = args.repo
    else:
        repo = os.getenv('GITHUB_REPOSITORY')
        if not repo:
            print('Please specify --repo, or setup GITHUB_REPOSITORY environment variable')
            exit(1)

    if args.token:
        token = args.token
    else:
        token = os.getenv('GITHUB_TOKEN')
        if not token:
            print('Please specify --token, or setup GITHUB_TOKEN environment variable')
            exit(1)

    delete_github_release(repo, args.tag, token)


if __name__ == '__main__':
    main()
