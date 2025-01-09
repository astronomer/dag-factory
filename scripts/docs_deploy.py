import subprocess
import sys

from packaging import version

import dagfactory


def deploy_docs(deploy_type: str):
    _version = version.parse(dagfactory.__version__)

    if deploy_type == "release":
        if _version.pre is not None:
            command = ["mike", "deploy", "--push", "dev"]
            # TODO: Remove L13-L18
            default_command = ["mike", "set-default", str(_version)]
            try:
                subprocess.run(default_command, capture_output=True, text=True, check=True)
            except subprocess.CalledProcessError as e:
                raise Exception(f"Error setting mike default: {e.stderr}")
        else:
            command = ["mike", "deploy", "--push", "--update-aliases", str(_version), "latest"]
            default_command = ["mike", "set-default", str(_version)]

            try:
                subprocess.run(default_command, capture_output=True, text=True, check=True)
            except subprocess.CalledProcessError as e:
                raise Exception(f"Error setting mike default: {e.stderr}")

    else:
        command = ["mike", "deploy", "--push", "dev"]

    try:
        subprocess.run(command, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        raise Exception(f"Error deploying: {e.stderr}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise Exception("Argument deploy type is required: 'dev' or 'release'")

    deploy_type = sys.argv[1]

    if deploy_type not in ["dev", "release"]:
        raise Exception("Invalid argument provided. Valid deploy types are 'dev' or 'release'.")

    deploy_docs(deploy_type)
