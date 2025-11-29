import json
import sys
import subprocess


def get_io_uring_commits(git_dir):
    try:
        git_cmd_base = ["git", "--git-dir",
                        f"{git_dir}/.git", "--work-tree", git_dir]

        # Get commits with 'io_uring' in the message and their details
        git_log_output = subprocess.check_output(
            git_cmd_base + [
                "log", "--grep=io_uring",  # "-n", "10",
                "--pretty=format:%H %ci %B%n##break##%n"
            ], text=True)
        print('got commit log')

        # Split the output into blocks separated by ##break##
        commits = git_log_output.strip().split("##break##")

        results = []

        for commit in commits:
            if not commit.strip():
                continue
            lines = commit.strip().split("\n")
            commit_hash, commit_date = lines[0].split(" ", 1)
            message = "\n".join(lines[1:]).strip()
            print(f'Commit: {commit_hash}')

            # Use git describe to find the nearest tag
            try:
                kernel_version_output = subprocess.check_output(
                    git_cmd_base + [
                        "describe", "--contains", commit_hash
                    ], text=True).strip()
            except subprocess.CalledProcessError:
                kernel_version_output = "No tag found"

            results.append({
                "commit": commit_hash,
                "message": message,
                "kernel_version": kernel_version_output.split('~', 1)[0],
                "release_date": commit_date
            })

        return results

    except subprocess.CalledProcessError as e:
        print(f"Error executing git command: {e}")
        return []


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <path_to_git_repo>")
        sys.exit(1)

    git_dir = sys.argv[1]
    commits_with_versions = get_io_uring_commits(git_dir)

    print("io_uring Commits with Kernel Versions:")
    for entry in commits_with_versions:
        print(f"Commit: {entry['commit']}")
        # print(f"Message: {entry['message']}")
        print(f"Kernel Version: {entry['kernel_version']}")
        print(f"Date: {entry['release_date']}")
        print("-")

    print('writing file')
    with open('commits.json', 'w') as f:
        json.dump(commits_with_versions, f, indent=4)
